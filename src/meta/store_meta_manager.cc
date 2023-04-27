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

#include "meta/store_meta_manager.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "butil/strings/stringprintf.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

namespace store {
std::shared_ptr<Region> Region::New(const pb::common::RegionDefinition& definition) {
  auto region = std::make_shared<Region>();
  region->inner_region_.set_id(definition.id());
  region->inner_region_.mutable_definition()->CopyFrom(definition);
  region->SetState(pb::common::StoreRegionState::NEW);
  return region;
}

std::string Region::Serialize() { return inner_region_.SerializeAsString(); }
void Region::DeSerialize(const std::string& data) {
  inner_region_.ParsePartialFromArray(data.data(), data.size());
  state_.store(inner_region_.state());
}

uint64_t Region::LeaderId() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.leader_id();
}
void Region::SetLeaderId(uint64_t leader_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.set_leader_id(leader_id);
}

const pb::common::Range& Region::Range() {
  BAIDU_SCOPED_LOCK(mutex_);
  return inner_region_.definition().range();
}
void Region::SetRange(const pb::common::Range& range) {
  BAIDU_SCOPED_LOCK(mutex_);
  inner_region_.mutable_definition()->mutable_range()->CopyFrom(range);
}

std::vector<pb::common::Peer> Region::Peers() const {
  std::vector<pb::common::Peer> peers(inner_region_.definition().peers().begin(),
                                      inner_region_.definition().peers().end());
  return peers;
}

void Region::SetPeers(std::vector<pb::common::Peer>& peers) {
  google::protobuf::RepeatedPtrField<pb::common::Peer> tmp_peers;
  tmp_peers.Add(peers.begin(), peers.end());

  inner_region_.mutable_definition()->mutable_peers()->Swap(&tmp_peers);
}

pb::common::StoreRegionState Region::State() const { return state_.load(std::memory_order_relaxed); }
void Region::SetState(pb::common::StoreRegionState state) {
  state_.store(state, std::memory_order_relaxed);
  inner_region_.set_state(state);
}

}  // namespace store

bool StoreServerMeta::Init() {
  auto* server = Server::GetInstance();

  std::shared_ptr<pb::common::Store> store = std::make_shared<pb::common::Store>();
  store->set_id(server->Id());
  store->mutable_keyring()->assign(server->Keyring());
  store->set_epoch(0);
  store->set_state(pb::common::STORE_NORMAL);
  auto* server_location = store->mutable_server_location();
  server_location->set_host(butil::ip2str(server->ServerEndpoint().ip).c_str());
  server_location->set_port(server->ServerEndpoint().port);
  auto* raf_location = store->mutable_raft_location();
  raf_location->set_host(butil::ip2str(server->RaftEndpoint().ip).c_str());
  raf_location->set_port(server->RaftEndpoint().port);

  DINGO_LOG(INFO) << "store server meta: " << store->ShortDebugString();
  AddStore(store);

  return true;
}

uint64_t StoreServerMeta::GetEpoch() const { return epoch_; }

StoreServerMeta& StoreServerMeta::SetEpoch(uint64_t epoch) {
  epoch_ = epoch;
  return *this;
}

bool StoreServerMeta::IsExist(uint64_t store_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  return stores_.find(store_id) != stores_.end();
}

void StoreServerMeta::AddStore(std::shared_ptr<pb::common::Store> store) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (stores_.find(store->id()) != stores_.end()) {
    DINGO_LOG(WARNING) << butil::StringPrintf("store %lu already exist!", store->id());
    return;
  }

  stores_.insert(std::make_pair(store->id(), store));
}

void StoreServerMeta::UpdateStore(std::shared_ptr<pb::common::Store> store) {
  BAIDU_SCOPED_LOCK(mutex_);
  epoch_ += 1;
  stores_.insert_or_assign(store->id(), store);
}
void StoreServerMeta::DeleteStore(uint64_t store_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  stores_.erase(store_id);
}

std::shared_ptr<pb::common::Store> StoreServerMeta::GetStore(uint64_t store_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = stores_.find(store_id);
  if (it == stores_.end()) {
    DINGO_LOG(WARNING) << butil::StringPrintf("region %lu not exist!", store_id);
    return nullptr;
  }

  return it->second;
}

std::map<uint64_t, std::shared_ptr<pb::common::Store>> StoreServerMeta::GetAllStore() {
  BAIDU_SCOPED_LOCK(mutex_);
  return stores_;
}

bool StoreRegionMeta::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "Scan store region meta failed!";
    return false;
  }
  DINGO_LOG(INFO) << "Init store region meta num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

uint64_t StoreRegionMeta::GetEpoch() { return 0; }

void StoreRegionMeta::AddRegion(store::RegionPtr region) {
  if (regions_.Get(region->Id()) != nullptr) {
    DINGO_LOG(WARNING) << butil::StringPrintf("region %lu already exist!", region->Id());
    return;
  }

  region->AppendHistoryState(pb::common::StoreRegionState::NEW);
  regions_.Put(region->Id(), region);

  if (meta_writer_ != nullptr) {
    meta_writer_->Put(TransformToKv(&region));
  }
}

void StoreRegionMeta::DeleteRegion(uint64_t region_id) {
  regions_.Erase(region_id);

  if (meta_writer_ != nullptr) {
    meta_writer_->Delete(GenKey(region_id));
  }
}

void StoreRegionMeta::UpdateRegion(store::RegionPtr region) {
  regions_.Put(region->Id(), region);

  if (meta_writer_ != nullptr) {
    meta_writer_->Put(TransformToKv(&region));
  }
}

void StoreRegionMeta::UpdateState(store::RegionPtr region, pb::common::StoreRegionState new_state) {
  assert(region != nullptr);

  bool successed = false;
  auto cur_state = region->State();
  switch (cur_state) {
    case pb::common::StoreRegionState::NEW:
      if (new_state == pb::common::StoreRegionState::NORMAL || new_state == pb::common::StoreRegionState::STANDBY) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::NORMAL:
      if (new_state == pb::common::StoreRegionState::STANDBY || new_state == pb::common::StoreRegionState::SPLITTING ||
          new_state == pb::common::StoreRegionState::MERGING || new_state == pb::common::StoreRegionState::DELETING ||
          new_state == pb::common::StoreRegionState::ORPHAN) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::STANDBY:
      if (new_state == pb::common::StoreRegionState::NORMAL || new_state == pb::common::StoreRegionState::ORPHAN) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::SPLITTING:
      if (new_state == pb::common::StoreRegionState::NORMAL) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::MERGING:
      if (new_state == pb::common::StoreRegionState::NORMAL) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::DELETING:
      if (new_state == pb::common::StoreRegionState::DELETED) {
        region->SetState(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::DELETED:
      break;
    default:
      DINGO_LOG(ERROR) << "Unknown region state " << pb::common::StoreRegionState_Name(cur_state);
      break;
  }

  if (successed) {
    region->AppendHistoryState(new_state);
    if (meta_writer_ != nullptr) {
      meta_writer_->Put(TransformToKv(&region));
    }
  }

  DINGO_LOG(DEBUG) << butil::StringPrintf(
      "Update region state %ld %s to %s %s", region->Id(), pb::common::StoreRegionState_Name(cur_state).c_str(),
      pb::common::StoreRegionState_Name(new_state).c_str(), (successed ? "true" : "false"));
}

void StoreRegionMeta::UpdateState(uint64_t region_id, pb::common::StoreRegionState new_state) {
  UpdateState(GetRegion(region_id), new_state);
}

void StoreRegionMeta::UpdateLeaderId(store::RegionPtr region, uint64_t leader_id) {
  assert(region != nullptr);

  region->SetLeaderId(leader_id);
}

void StoreRegionMeta::UpdateLeaderId(uint64_t region_id, uint64_t leader_id) {
  UpdateLeaderId(GetRegion(region_id), leader_id);
}

bool StoreRegionMeta::IsExistRegion(uint64_t region_id) { return GetRegion(region_id) != nullptr; }

store::RegionPtr StoreRegionMeta::GetRegion(uint64_t region_id) {
  auto region = regions_.Get(region_id);
  if (region == nullptr) {
    DINGO_LOG(WARNING) << butil::StringPrintf("region %lu not exist!", region_id);
    return nullptr;
  }

  return region;
}

std::vector<store::RegionPtr> StoreRegionMeta::GetAllRegion() {
  std::vector<store::RegionPtr> regions;
  regions.reserve(regions_.Size());

  if (regions_.GetAllValues(regions) < 0) {
    DINGO_LOG(ERROR) << "Get all regions failed!";
    return regions;
  }

  return regions;
}

std::vector<store::RegionPtr> StoreRegionMeta::GetAllAliveRegion() {
  std::vector<store::RegionPtr> regions;
  regions.reserve(regions_.Size());

  if (regions_.GetAllValues(regions, [](store::RegionPtr region) -> bool {
        return region->State() != pb::common::StoreRegionState::DELETED;
      }) < 0) {
    DINGO_LOG(ERROR) << "Get all regions failed!";
    return regions;
  }

  return regions;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::TransformToKv(void* obj) {
  auto region = *static_cast<store::RegionPtr*>(obj);
  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region->Id()));
  kv->set_value(region->Serialize());

  return kv;
}

void StoreRegionMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    auto region = std::make_shared<store::Region>();
    region->DeSerialize(kv.value());
    regions_.Put(region_id, region);
  }
}

bool StoreRaftMeta::Init() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(Prefix(), kvs)) {
    DINGO_LOG(ERROR) << "Scan store raft meta failed!";
    return false;
  }
  DINGO_LOG(INFO) << "Init store raft meta num: " << kvs.size();

  if (!kvs.empty()) {
    TransformFromKv(kvs);
  }
  return true;
}

StoreRaftMeta::RaftMetaPtr StoreRaftMeta::NewRaftMeta(uint64_t region_id) {
  auto raft_meta = std::make_shared<pb::store_internal::RaftMeta>();
  raft_meta->set_region_id(region_id);
  raft_meta->set_term(0);
  raft_meta->set_applied_index(0);
  return raft_meta;
}

void StoreRaftMeta::AddRaftMeta(RaftMetaPtr raft_meta) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    DINGO_LOG(INFO) << "Add raft meta " << raft_meta->region_id();
    if (raft_metas_.find(raft_meta->region_id()) != raft_metas_.end()) {
      DINGO_LOG(WARNING) << butil::StringPrintf("raft meta %lu already exist!", raft_meta->region_id());
      return;
    }

    raft_metas_.insert(std::make_pair(raft_meta->region_id(), raft_meta));
  }

  meta_writer_->Put(TransformToKv(&raft_meta));
}

void StoreRaftMeta::UpdateRaftMeta(RaftMetaPtr raft_meta) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    raft_metas_.insert_or_assign(raft_meta->region_id(), raft_meta);
  }

  meta_writer_->Put(TransformToKv(&raft_meta));
}

void StoreRaftMeta::DeleteRaftMeta(uint64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    raft_metas_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

StoreRaftMeta::RaftMetaPtr StoreRaftMeta::GetRaftMeta(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = raft_metas_.find(region_id);
  if (it == raft_metas_.end()) {
    DINGO_LOG(WARNING) << butil::StringPrintf("raft meta %lu not exist!", region_id);
    return nullptr;
  }

  return it->second;
}

std::vector<StoreRaftMeta::RaftMetaPtr> StoreRaftMeta::GetAllRaftMeta() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<StoreRaftMeta::RaftMetaPtr> raft_metas;
  for (auto& [_, raft_meta] : raft_metas_) {
    raft_metas.push_back(raft_meta);
  }

  return raft_metas;
}

std::shared_ptr<pb::common::KeyValue> StoreRaftMeta::TransformToKv(void* obj) {
  auto raft_meta = *static_cast<RaftMetaPtr*>(obj);
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(raft_meta->region_id()));
  kv->set_value(raft_meta->SerializeAsString());

  return kv;
}

void StoreRaftMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    RaftMetaPtr raft_meta = std::make_shared<pb::store_internal::RaftMeta>();
    raft_meta->ParsePartialFromArray(kv.value().data(), kv.value().size());
    raft_metas_.insert_or_assign(region_id, raft_meta);
  }
}

bool StoreMetaManager::Init() {
  if (!server_meta_->Init()) {
    DINGO_LOG(ERROR) << "Init store server meta failed!";
    return false;
  }

  if (!region_meta_->Init()) {
    DINGO_LOG(ERROR) << "Init store region meta failed!";
    return false;
  }

  if (!raft_meta_->Init()) {
    DINGO_LOG(ERROR) << "Init store raft meta failed!";
    return false;
  }

  return true;
}

}  // namespace dingodb
