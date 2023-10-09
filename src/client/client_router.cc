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

#include "client/client_router.h"

#include <memory>
#include <utility>

#include "client/client_interation.h"

namespace client {

// Query region
static dingodb::pb::common::Region SendQueryRegion(int64_t region_id) {
  dingodb::pb::coordinator::QueryRegionRequest request;
  dingodb::pb::coordinator::QueryRegionResponse response;

  request.set_region_id(region_id);

  InteractionManager::GetInstance().SendRequestWithoutContext("CoordinatorService", "QueryRegion", request, response);

  return response.region();
}

RegionEntry::RegionEntry(const dingodb::pb::common::Region& region) : region_(region), is_dirty_(false) {}

std::shared_ptr<RegionEntry> RegionEntry::New(dingodb::pb::common::Region region) {
  return std::make_shared<RegionEntry>(region);
}

bool RegionEntry::IsDirty() { return is_dirty_.load(); }

void RegionEntry::SetDirty(bool dirty) { is_dirty_.store(dirty); }

int64_t RegionEntry::RegionId() { return region_.id(); }

int64_t RegionEntry::PartitionId() { return region_.definition().part_id(); }

int64_t RegionEntry::TableId() { return region_.definition().table_id(); }

dingodb::pb::common::Region& RegionEntry::Region() { return region_; }
void RegionEntry::SetRegion(const dingodb::pb::common::Region& region) { region_ = region; }

const dingodb::pb::common::Range& RegionEntry::Range() { return region_.definition().range(); }
void RegionEntry::SetRange(const dingodb::pb::common::Range& range) {
  *region_.mutable_definition()->mutable_range() = range;
}

const dingodb::pb::common::RegionEpoch& RegionEntry::Epoch() { return region_.definition().epoch(); }
void RegionEntry::SetEpoch(const dingodb::pb::common::RegionEpoch& epoch) {
  *region_.mutable_definition()->mutable_epoch() = epoch;
}

std::vector<std::string> RegionEntry::GetAddrs() {
  std::vector<std::string> addrs;
  for (const auto& peer : region_.definition().peers()) {
    const auto& location = peer.server_location();
    addrs.push_back(fmt::format("{}:{}", location.host(), location.port()));
  }

  return addrs;
}

void RegionEntry::SetPeers(const dingodb::pb::error::StoreRegionInfo& region_info) {
  *region_.mutable_definition()->mutable_peers() = region_info.peers();
}

dingodb::pb::store::Context RegionEntry::GenConext() {
  dingodb::pb::store::Context ctx;
  ctx.set_region_id(region_.id());
  *ctx.mutable_region_epoch() = region_.definition().epoch();
  return ctx;
}

RegionRouter::RegionRouter() { bthread_mutex_init(&mutex_, nullptr); }
RegionRouter::~RegionRouter() { bthread_mutex_destroy(&mutex_); }

RegionRouter& RegionRouter::GetInstance() {
  static RegionRouter instance;
  return instance;
}

void RegionRouter::AddRegionEntry(const dingodb::pb::common::Region& region) {
  BAIDU_SCOPED_LOCK(mutex_);

  route_map_.insert_or_assign(region.definition().range().start_key(), RegionEntry::New(region));
}

void RegionRouter::AddRegionEntry(RegionEntryPtr region) {
  BAIDU_SCOPED_LOCK(mutex_);

  route_map_.insert_or_assign(region->Range().start_key(), region);
}

RegionEntryPtr RegionRouter::AddRegionEntry(int64_t region_id) {
  auto region = SendQueryRegion(region_id);
  if (region.id() == 0) {
    return nullptr;
  }

  auto region_entry = RegionEntry::New(region);
  AddRegionEntry(region_entry);
  return region_entry;
}

void RegionRouter::UpdateRegionEntry(const dingodb::pb::error::StoreRegionInfo& region_info) {
  auto region_entry = QueryRegionEntry(region_info.region_id());
  if (region_entry != nullptr) {
    region_entry->SetPeers(region_info);
    region_entry->SetRange(region_info.current_range());
    region_entry->SetEpoch(region_info.current_region_epoch());
  }
}

RegionEntryPtr RegionRouter::QueryRegionEntry(const std::string& key) {
  BAIDU_SCOPED_LOCK(mutex_);

  auto it = route_map_.lower_bound(key);
  if (it == route_map_.end()) {
    return nullptr;
  }
  auto region_entry = it->second;
  if (region_entry->IsDirty()) {
  }

  const auto& range = region_entry->Range();
  if (key.compare(range.start_key()) >= 0 && key.compare(range.end_key()) < 0) {
    return region_entry;
  }

  return nullptr;
}

RegionEntryPtr RegionRouter::QueryRegionEntry(int64_t region_id) {
  RegionEntryPtr region_entry;

  {
    BAIDU_SCOPED_LOCK(mutex_);

    for (auto& [_, temp_region_entry] : route_map_) {
      if (temp_region_entry->RegionId() == region_id) {
        region_entry = temp_region_entry;
        break;
      }
    }
  }

  if (region_entry == nullptr) {
    return AddRegionEntry(region_id);
  } else {
    if (region_entry->IsDirty()) {
      UpdateRegion(region_entry);
    }
  }

  return region_entry;
}

std::vector<RegionEntryPtr> RegionRouter::QueryRegionEntryByTable(int64_t table_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<RegionEntryPtr> region_entries;
  for (auto& [_, region_entry] : route_map_) {
    if (region_entry->TableId() == table_id) {
      if (region_entry->IsDirty()) {
        UpdateRegion(region_entry);
      }
      region_entries.push_back(region_entry);
    }
  }

  return region_entries;
}

std::vector<RegionEntryPtr> RegionRouter::QueryRegionEntryByPartition(int64_t partition_id) {
  BAIDU_SCOPED_LOCK(mutex_);

  std::vector<RegionEntryPtr> region_entries;
  for (auto& [_, region_entry] : route_map_) {
    if (region_entry->PartitionId() == partition_id) {
      if (region_entry->IsDirty()) {
        UpdateRegion(region_entry);
      }
      region_entries.push_back(region_entry);
    }
  }

  return region_entries;
}

dingodb::pb::store::Context RegionRouter::GenConext(int64_t region_id) {
  auto region_entry = QueryRegionEntry(region_id);
  if (region_entry != nullptr) {
    return region_entry->GenConext();
  }

  return {};
}

bool RegionRouter::UpdateRegion(RegionEntryPtr region_entry) {
  auto region = SendQueryRegion(region_entry->RegionId());
  if (region.id() == 0) {
    return false;
  }

  region_entry->SetRegion(region);
  region_entry->SetDirty(false);
  return true;
}

}  // namespace client