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

#include "butil/strings/stringprintf.h"
#include "glog/logging.h"
#include "server/server.h"

namespace dingodb {

StoreServerMeta::StoreServerMeta() {}

bool StoreServerMeta::Init() {
  auto* server = Server::GetInstance();

  std::shared_ptr<pb::common::Store> store = std::make_shared<pb::common::Store>();
  store->set_id(server->Id());
  store->set_epoch(0);
  store->set_state(pb::common::STORE_NORMAL);
  auto* server_location = store->mutable_server_location();
  server_location->set_host(butil::ip2str(server->ServerEndpoint().ip).c_str());
  server_location->set_port(server->ServerEndpoint().port);
  auto* raf_location = store->mutable_raft_location();
  raf_location->set_host(butil::ip2str(server->RaftEndpoint().ip).c_str());
  raf_location->set_port(server->RaftEndpoint().port);

  LOG(INFO) << "store server meta: " << store->ShortDebugString();
  AddStore(store);

  return true;
}

uint64_t StoreServerMeta::GetEpoch() const { return epoch_; }

StoreServerMeta& StoreServerMeta::SetEpoch(uint64_t epoch) {
  epoch_ = epoch;
  return *this;
}

bool StoreServerMeta::IsExist(uint64_t store_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return stores_.find(store_id) != stores_.end();
}

void StoreServerMeta::AddStore(std::shared_ptr<pb::common::Store> store) {
  if (IsExist(store->id())) {
    LOG(WARNING) << butil::StringPrintf("store %lu already exist!", store->id());
    return;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  stores_.insert(std::make_pair(store->id(), store));
}
void StoreServerMeta::UpdateStore(std::shared_ptr<pb::common::Store> store) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  epoch_ += 1;
  stores_.insert_or_assign(store->id(), store);
}
void StoreServerMeta::DeleteStore(uint64_t store_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  stores_.erase(store_id);
}

std::shared_ptr<pb::common::Store> StoreServerMeta::GetStore(uint64_t store_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = stores_.find(store_id);
  if (it == stores_.end()) {
    LOG(WARNING) << butil::StringPrintf("region %lu not exist!", store_id);
    return nullptr;
  }

  return it->second;
}

std::map<uint64_t, std::shared_ptr<pb::common::Store>> StoreServerMeta::GetAllStore() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return stores_;
}

bool StoreRegionMeta::Init() { return true; }

bool StoreRegionMeta::Recover(const std::vector<pb::common::KeyValue>& kvs) {
  TransformFromKv(kvs);
  return true;
}

uint64_t StoreRegionMeta::GetEpoch() const { return epoch_; }

bool StoreRegionMeta::IsExist(uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return regions_.find(region_id) != regions_.end();
}

void StoreRegionMeta::AddRegion(const std::shared_ptr<pb::common::Region> region) {
  if (IsExist(region->id())) {
    LOG(WARNING) << butil::StringPrintf("region %lu already exist!", region->id());
    return;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  regions_.insert(std::make_pair(region->id(), region));
}

void StoreRegionMeta::UpdateRegion(std::shared_ptr<pb::common::Region> region) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  region->set_epoch(region->epoch() + 1);
  regions_.insert_or_assign(region->id(), region);
}

void StoreRegionMeta::DeleteRegion(uint64_t region_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  regions_.erase(region_id);
}

std::shared_ptr<pb::common::Region> StoreRegionMeta::GetRegion(uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    LOG(WARNING) << butil::StringPrintf("region %lu not exist!", region_id);
    return nullptr;
  }

  return it->second;
}

std::map<uint64_t, std::shared_ptr<pb::common::Region>> StoreRegionMeta::GetAllRegion() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return regions_;
}

uint64_t StoreRegionMeta::ParseRegionId(const std::string& str) {
  if (str.size() <= prefix_.size()) {
    LOG(ERROR) << "Parse region id failed, invalid str " << str;
    return 0;
  }

  std::string s(str.c_str() + prefix_.size() + 1);
  try {
    return std::stoull(s, nullptr, 10);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "string to uint64_t failed: " << e.what();
  }

  return 0;
}

std::string StoreRegionMeta::GenKey(uint64_t region_id) {
  return butil::StringPrintf("%s_%lu", prefix_.c_str(), region_id);
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::TransformToKv(const std::shared_ptr<pb::common::Region> region) {
  std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenKey(region->id()));
  kv->set_value(region->SerializeAsString());

  return kv;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::TransformToKv(uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    return nullptr;
  }

  return TransformToKv(it->second);
}

std::vector<std::shared_ptr<pb::common::KeyValue>> StoreRegionMeta::TransformToKvtWithDelta() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<std::shared_ptr<pb::common::KeyValue>> kvs;
  for (auto region_id : changed_regions_) {
    auto it = regions_.find(region_id);
    if (it != regions_.end()) {
      std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
      kv->set_key(GenKey(it->first));
      kv->set_value(it->second->SerializeAsString());
      kvs.push_back(kv);
    }
  }

  return kvs;
}

std::vector<std::shared_ptr<pb::common::KeyValue>> StoreRegionMeta::TransformToKvWithAll() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<std::shared_ptr<pb::common::KeyValue>> kvs;
  for (const auto& it : regions_) {
    std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
    kv->set_key(GenKey(it.first));
    kv->set_value(it.second->SerializeAsString());
    kvs.push_back(kv);
  }

  return kvs;
}

void StoreRegionMeta::TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  for (const auto& kv : kvs) {
    uint64_t region_id = ParseRegionId(kv.key());
    std::shared_ptr<pb::common::Region> region = std::make_shared<pb::common::Region>();
    region->ParsePartialFromArray(kv.value().data(), kv.value().size());
    regions_.insert_or_assign(region_id, region);
  }
}

StoreMetaManager::StoreMetaManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
    : meta_reader_(meta_reader),
      meta_writer_(meta_writer),
      server_meta_(std::make_unique<StoreServerMeta>()),
      region_meta_(std::make_unique<StoreRegionMeta>()) {}

bool StoreMetaManager::Init() {
  if (!server_meta_->Init()) {
    LOG(ERROR) << "Init store server meta failed!";
    return false;
  }

  if (!region_meta_->Init()) {
    LOG(ERROR) << "Init store region meta failed!";
    return false;
  }

  return true;
}

bool StoreMetaManager::Recover() {
  std::vector<pb::common::KeyValue> kvs;
  if (!meta_reader_->Scan(region_meta_->prefix(), kvs)) {
    return false;
  }

  if (!region_meta_->Recover(kvs)) {
    return false;
  }

  return true;
}

uint64_t StoreMetaManager::GetServerEpoch() { return server_meta_->GetEpoch(); }

uint64_t StoreMetaManager::GetRegionEpoch() { return region_meta_->GetEpoch(); }

bool StoreMetaManager::IsExistStore(uint64_t store_id) { return server_meta_->IsExist(store_id); }

void StoreMetaManager::AddStore(std::shared_ptr<pb::common::Store> store) {
  LOG(INFO) << "StoreMeta add store, store_id " << store->id();
  server_meta_->AddStore(store);
}

void StoreMetaManager::UpdateStore(std::shared_ptr<pb::common::Store> store) { server_meta_->UpdateStore(store); }

void StoreMetaManager::DeleteStore(uint64_t store_id) { server_meta_->DeleteStore(store_id); }

std::shared_ptr<pb::common::Store> StoreMetaManager::GetStore(uint64_t store_id) {
  return server_meta_->GetStore(store_id);
}

std::map<uint64_t, std::shared_ptr<pb::common::Store>> StoreMetaManager::GetAllStore() {
  return server_meta_->GetAllStore();
}

bool StoreMetaManager::IsExistRegion(uint64_t region_id) { return region_meta_->IsExist(region_id); }

void StoreMetaManager::AddRegion(const std::shared_ptr<pb::common::Region> region) {
  LOG(INFO) << "StoreMeta add region, region_id " << region->id();
  region_meta_->AddRegion(region);
  meta_writer_->Put(region_meta_->TransformToKv(region));
}

void StoreMetaManager::UpdateRegion(std::shared_ptr<pb::common::Region> region) {
  region_meta_->UpdateRegion(region);
  meta_writer_->Put(region_meta_->TransformToKv(region));
}

void StoreMetaManager::DeleteRegion(uint64_t region_id) {
  region_meta_->DeleteRegion(region_id);
  meta_writer_->Delete(region_meta_->GenKey(region_id));
}

std::shared_ptr<pb::common::Region> StoreMetaManager::GetRegion(uint64_t region_id) {
  return region_meta_->GetRegion(region_id);
}

std::map<uint64_t, std::shared_ptr<pb::common::Region>> StoreMetaManager::GetAllRegion() {
  return region_meta_->GetAllRegion();
}

}  // namespace dingodb