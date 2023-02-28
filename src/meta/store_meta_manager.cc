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

namespace dingodb {

StoreServerMeta::StoreServerMeta() {
  store_ = std::make_shared<pb::common::Store>();
}

StoreServerMeta::~StoreServerMeta() {}

uint64_t StoreServerMeta::GetEpoch() { return epoch_; }

StoreServerMeta& StoreServerMeta::SetEpoch(uint64_t epoch) {
  epoch_ = epoch;
  return *this;
}

StoreServerMeta& StoreServerMeta::SetId(uint64_t id) {
  store_->set_id(id);
  return *this;
}

StoreServerMeta& StoreServerMeta::SetStatus(pb::common::StoreStatus status) {
  store_->set_status(status);
  return *this;
}

StoreServerMeta& StoreServerMeta::SetServerLocation(
    const butil::EndPoint&& endpoint) {
  auto location = store_->mutable_server_location();
  location->set_host(butil::ip2str(endpoint.ip).c_str());
  location->set_port(endpoint.port);
}

StoreServerMeta& StoreServerMeta::SetRaftLocation(
    const butil::EndPoint&& endpoint) {
  auto location = store_->mutable_raft_location();
  location->set_host(butil::ip2str(endpoint.ip).c_str());
  location->set_port(endpoint.port);
}

std::shared_ptr<pb::common::Store> StoreServerMeta::GetStore() {
  return store_;
}

StoreRegionMeta::StoreRegionMeta() {}
StoreRegionMeta::~StoreRegionMeta() {}

uint64_t StoreRegionMeta::GetEpoch() { return epoch_; }

bool StoreRegionMeta::IsExist(uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return regions_.find(region_id) != regions_.end();
}

void StoreRegionMeta::AddRegion(uint64_t region_id,
                                const dingodb::pb::common::Region& region) {
  if (IsExist(region_id)) {
    LOG(WARNING) << butil::StringPrintf("region %lu already exist!", region_id);
    return;
  }

  std::unique_lock<std::shared_mutex> lock(mutex_);
  regions_.insert(std::make_pair(
      region_id, std::make_shared<dingodb::pb::common::Region>(region)));
}

std::shared_ptr<dingodb::pb::common::Region> StoreRegionMeta::GetRegion(
    uint64_t region_id) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = regions_.find(region_id);
  if (it == regions_.end()) {
    LOG(WARNING) << butil::StringPrintf("region %lu not exist!", region_id);
    return nullptr;
  }

  return it->second;
}

std::vector<std::shared_ptr<dingodb::pb::common::Region> >
StoreRegionMeta::GetAllRegion() {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  std::vector<std::shared_ptr<dingodb::pb::common::Region> > result;
  for (auto it = regions_.begin(); it != regions_.end(); ++it) {
    result.push_back(it->second);
  }

  return result;
}

std::shared_ptr<pb::common::KeyValue> StoreRegionMeta::Transform() {}
std::vector<pb::common::KeyValue> StoreRegionMeta::TransformAll() {}

std::vector<pb::common::KeyValue> StoreRegionMeta::TransformDelta() {}

StoreMetaManager::StoreMetaManager()
    : server_meta_(std::make_unique<StoreServerMeta>()),
      region_meta_(std::make_unique<StoreRegionMeta>()) {}

StoreMetaManager::~StoreMetaManager() {}

uint64_t StoreMetaManager::GetServerEpoch() { return server_meta_->GetEpoch(); }

uint64_t StoreMetaManager::GetRegionEpoch() { return region_meta_->GetEpoch(); }

std::shared_ptr<pb::common::Store> StoreMetaManager::GetStore() {
  return server_meta_->GetStore();
}

std::vector<std::shared_ptr<pb::common::Region> >
StoreMetaManager::GetAllRegion() {
  return region_meta_->GetAllRegion();
}

void StoreMetaManager::AddRegion(uint64_t region_id,
                                 const pb::common::Region& region) {
  region_meta_->AddRegion(region_id, region);
}

}  // namespace dingodb