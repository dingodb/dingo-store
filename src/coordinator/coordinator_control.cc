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

#include "coordinator/coordinator_control.h"

#include <cstdint>
#include <string>
#include <vector>

#include "google/protobuf/unknown_field_set.h"

namespace dingodb {
void CoordinatorControl::Init() {
  next_coordinator_id_ = 1;
  next_store_id_ = 1;
  next_region_id_ = 1;
  next_schema_id_ = 1;
  next_table_id_ = 1;
  next_partition_id_ = 1;
}

uint64_t CoordinatorControl::CreateCoordinatorId() {
  return next_coordinator_id_++;
}

uint64_t CoordinatorControl::CreateStoreId() { return next_store_id_++; }
uint64_t CoordinatorControl::CreateRegionId() { return next_region_id_++; }
uint64_t CoordinatorControl::CreateSchemaId() { return next_schema_id_++; }
uint64_t CoordinatorControl::CreateTableId() { return next_table_id_++; }
uint64_t CoordinatorControl::CreatePartitionId() {
  return next_partition_id_++;
}

// TODO: data persistence
uint64_t CoordinatorControl::UpdateStoreMap(const pb::common::Store& store) {
  bool need_update_epoch = false;
  for (int i = 0; i < store_map_.stores_size(); i++) {
    auto* old_store = store_map_.mutable_stores(i);
    if (old_store->id() == store.id()) {
      // update old store properties
      if (old_store->status() != store.status()) {
        LOG(INFO) << "STORE STATUS CHANGE store_id = " << store.id()
                  << " old status = " << old_store->status()
                  << " new status = " << store.status();
        store_map_.set_epoch(store_map_.epoch() + 1);
        need_update_epoch = true;
      }
      old_store->CopyFrom(store);
      break;
    }
  }

  if (!need_update_epoch) {
    // create new store
    auto* new_store = store_map_.add_stores();
    new_store->CopyFrom(store);
  }

  LOG(INFO) << "UpdateStoreMap " << store_map_.DebugString();

  return store_map_.epoch();
}

bool CoordinatorControl::UpdateOneRegionMap(const pb::common::Region& region) {
  bool need_to_add_region = true;
  bool need_to_update_epoch = false;

  for (int i = 0; i < region_map_.regions_size(); i++) {
    auto* old_region = region_map_.mutable_regions(i);
    if (old_region->id() == region.id()) {
      if (old_region->status() != region.status()) {
        LOG(INFO) << "REGION STATUS CHANGE region_id = " << region.id()
                  << " old status = " << old_region->status()
                  << " new status = " << region.status();
      }
      old_region->CopyFrom(region);
      need_to_add_region = false;
      need_to_update_epoch = true;
      break;
    }
  }

  if (need_to_add_region) {
    // add new region to regionmap (may only used for testing, not exist in
    // production env)
    auto* new_region = region_map_.add_regions();
    new_region->CopyFrom(region);
    need_to_update_epoch = true;
  }

  return need_to_update_epoch;
}

// TODO: data persistence
uint64_t CoordinatorControl::UpdateRegionMap(const pb::common::Region& region) {
  if (UpdateOneRegionMap(region)) {
    region_map_.set_epoch(region_map_.epoch() + 1);
  }

  return region_map_.epoch();
}

// TODO: data persistence
uint64_t CoordinatorControl::UpdateRegionMapMulti(
    std::vector<pb::common::Region> regions) {
  bool need_to_update_epoch = false;

  for (const auto& region : regions) {
    if (UpdateOneRegionMap(region)) {
      need_to_update_epoch = true;
    }
  }

  if (need_to_update_epoch) {
    region_map_.set_epoch(region_map_.epoch());
  }

  LOG(INFO) << "UpdateRegionMapMulti " << region_map_.DebugString();

  return region_map_.epoch();
}

const pb::common::StoreMap& CoordinatorControl::GetStoreMap() {
  return this->store_map_;
}

const pb::common::RegionMap& CoordinatorControl::GetRegionMap() {
  return this->region_map_;
}

int CoordinatorControl::CreateStore(uint64_t cluster_id, uint64_t& store_id,
                                    std::string& password) {
  if (cluster_id > 0) {
    store_id = CreateStoreId();
    password = "TO_BE_CONTINUED";

    auto* store = store_map_.add_stores();
    store->set_id(store_id);

    return 0;
  } else {
    return -1;
  }
}
}  // namespace dingodb