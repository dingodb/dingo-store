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

#include <cstddef>
#include <cstdint>

#include "butil/strings/stringprintf.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

StoreServerMeta::StoreServerMeta() { bthread_mutex_init(&mutex_, nullptr); };
StoreServerMeta::~StoreServerMeta() { bthread_mutex_destroy(&mutex_); };

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

bool StoreRegionMeta::IsExistRegion(uint64_t region_id) { return GetRegion(region_id) != nullptr; }

void StoreRegionMeta::AddRegion(std::shared_ptr<pb::store_internal::Region> region) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (regions_.find(region->id()) != regions_.end()) {
      DINGO_LOG(WARNING) << butil::StringPrintf("region %lu already exist!", region->id());
      return;
    }

    region->add_history_states(pb::common::StoreRegionState::NEW);
    regions_.insert(std::make_pair(region->id(), region));
  }

  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::DeleteRegion(uint64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    regions_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

void StoreRegionMeta::UpdateRegion(std::shared_ptr<pb::store_internal::Region> region) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    regions_.insert_or_assign(region->id(), region);
  }

  meta_writer_->Put(TransformToKv(region));
}

void StoreRegionMeta::UpdateState(std::shared_ptr<pb::store_internal::Region> region,
                                  pb::common::StoreRegionState new_state) {
  bool successed = false;
  auto cur_state = region->state();
  switch (cur_state) {
    case pb::common::StoreRegionState::NEW:
      if (new_state == pb::common::StoreRegionState::NORMAL || new_state == pb::common::StoreRegionState::STANDBY) {
        region->set_state(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::NORMAL:
      if (new_state == pb::common::StoreRegionState::STANDBY || new_state == pb::common::StoreRegionState::SPLITTING ||
          new_state == pb::common::StoreRegionState::MERGING || new_state == pb::common::StoreRegionState::DELETING) {
        region->set_state(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::STANDBY:
      if (new_state == pb::common::StoreRegionState::NORMAL) {
        region->set_state(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::SPLITTING:
      if (new_state == pb::common::StoreRegionState::NORMAL) {
        region->set_state(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::MERGING:
      if (new_state == pb::common::StoreRegionState::NORMAL) {
        region->set_state(new_state);
        successed = true;
      }
      break;
    case pb::common::StoreRegionState::DELETING:
      if (new_state == pb::common::StoreRegionState::DELETED) {
        region->set_state(new_state);
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
    region->add_history_states(new_state);
    meta_writer_->Put(TransformToKv(region));
  }

  DINGO_LOG(DEBUG) << butil::StringPrintf(
      "Update region state %ld %s to %s %s", region->id(), pb::common::StoreRegionState_Name(cur_state).c_str(),
      pb::common::StoreRegionState_Name(new_state).c_str(), (successed ? "true" : "false"));
}

void StoreRegionMeta::UpdateState(uint64_t region_id, pb::common::StoreRegionState new_state) {
  UpdateState(GetRegion(region_id), new_state);
}

void StoreRegionMeta::UpdateLeaderId(std::shared_ptr<pb::store_internal::Region> region, uint64_t leader_id) {
  region->set_leader_id(leader_id);
}

void StoreRegionMeta::UpdateLeaderId(uint64_t region_id, uint64_t leader_id) {
  UpdateLeaderId(GetRegion(region_id), leader_id);
}

std::shared_ptr<pb::store_internal::Region> StoreRegionMeta::GetRegion(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    DINGO_LOG(WARNING) << butil::StringPrintf("region %lu not exist!", region_id);
    return nullptr;
  }

  return it->second;
}

std::vector<std::shared_ptr<pb::store_internal::Region>> StoreRegionMeta::GetAllRegion() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::store_internal::Region>> regions;
  regions.reserve(regions_.size());
  for (auto [_, region] : regions_) {
    regions.push_back(region);
  }

  return regions;
}

std::vector<std::shared_ptr<pb::store_internal::Region>> StoreRegionMeta::GetAllAliveRegion() {
  BAIDU_SCOPED_LOCK(mutex_);
  std::vector<std::shared_ptr<pb::store_internal::Region>> regions;
  regions.reserve(regions_.size());
  for (auto [_, region] : regions_) {
    if (region->state() != pb::common::StoreRegionState::DELETED) {
      regions.push_back(region);
    }
  }

  return regions;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::TransformToKv(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    return nullptr;
  }

  return TransformToKv(it->second);
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::TransformToKv(std::shared_ptr<google::protobuf::Message> obj) {
  auto region = std::dynamic_pointer_cast<pb::store_internal::Region>(obj);
  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region->id()));
  kv->set_value(region->SerializeAsString());

  return kv;
}

void StoreRegionMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    auto region = std::make_shared<pb::store_internal::Region>();
    region->ParsePartialFromArray(kv.value().data(), kv.value().size());
    regions_.insert_or_assign(region_id, region);
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

std::shared_ptr<pb::store_internal::RaftMeta> StoreRaftMeta::NewRaftMeta(uint64_t region_id) {
  auto raft_meta = std::make_shared<pb::store_internal::RaftMeta>();
  raft_meta->set_region_id(region_id);
  raft_meta->set_term(0);
  raft_meta->set_applied_index(0);
  return raft_meta;
}

void StoreRaftMeta::AddRaftMeta(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    DINGO_LOG(INFO) << "Add raft meta " << raft_meta->region_id();
    if (raft_metas_.find(raft_meta->region_id()) != raft_metas_.end()) {
      DINGO_LOG(WARNING) << butil::StringPrintf("raft meta %lu already exist!", raft_meta->region_id());
      return;
    }

    raft_metas_.insert(std::make_pair(raft_meta->region_id(), raft_meta));
  }

  meta_writer_->Put(TransformToKv(raft_meta));
}

void StoreRaftMeta::UpdateRaftMeta(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    raft_metas_.insert_or_assign(raft_meta->region_id(), raft_meta);
  }

  meta_writer_->Put(TransformToKv(raft_meta));
}

void StoreRaftMeta::DeleteRaftMeta(uint64_t region_id) {
  {
    BAIDU_SCOPED_LOCK(mutex_);
    raft_metas_.erase(region_id);
  }

  meta_writer_->Delete(GenKey(region_id));
}

std::shared_ptr<pb::store_internal::RaftMeta> StoreRaftMeta::GetRaftMeta(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = raft_metas_.find(region_id);
  if (it == raft_metas_.end()) {
    DINGO_LOG(WARNING) << butil::StringPrintf("raft meta %lu not exist!", region_id);
    return nullptr;
  }

  return it->second;
}

std::shared_ptr<pb::common::KeyValue> StoreRaftMeta::TransformToKv(
    const std::shared_ptr<google::protobuf::Message> obj) {
  auto raft_meta = std::dynamic_pointer_cast<pb::store_internal::RaftMeta>(obj);
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(raft_meta->region_id()));
  kv->set_value(raft_meta->SerializeAsString());

  return kv;
}

std::shared_ptr<pb::common::KeyValue> StoreRaftMeta::TransformToKv(uint64_t region_id) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto it = raft_metas_.find(region_id);
  if (it == raft_metas_.end()) {
    return nullptr;
  }

  return TransformToKv(it->second);
}

void StoreRaftMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  BAIDU_SCOPED_LOCK(mutex_);
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    std::shared_ptr<pb::store_internal::RaftMeta> raft_meta = std::make_shared<pb::store_internal::RaftMeta>();
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
