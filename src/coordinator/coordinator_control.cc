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
#include <utility>
#include <vector>

#include "google/protobuf/unknown_field_set.h"
#include "proto/common.pb.h"

namespace dingodb {
void CoordinatorControl::Init() {
  next_coordinator_id_ = 1;
  next_store_id_ = 1;
  next_region_id_ = 1;
  next_table_id_ = 1;
  next_partition_id_ = 1;

  // root=0 meta=1 dingo=2, other schema begins from 3
  next_schema_id_ = 3;

  // init schema_map_ at innocent cluster
  if (schema_map_.empty()) {
    pb::common::Schema root_schema;
    root_schema.set_id(0);
    root_schema.set_name("root");

    // meta schema
    pb::common::Schema meta_schema;
    meta_schema.set_id(1);
    meta_schema.set_name("meta");
    root_schema.add_schema_ids(1);

    // dingo schema
    pb::common::Schema dingo_schema;
    dingo_schema.set_id(2);
    dingo_schema.set_name("dingo");
    root_schema.add_schema_ids(2);

    // add the initial schemas to schema_map_
    // TODO: data persistence
    schema_map_.insert(std::make_pair(0, root_schema));
    schema_map_.insert(std::make_pair(1, meta_schema));
    schema_map_.insert(std::make_pair(2, dingo_schema));

    LOG(INFO) << "init schema_map_ finished";
  }
}

uint64_t CoordinatorControl::CreateCoordinatorId() {
  return next_coordinator_id_++;
}

// TODO: check name comflicts before create new schema
int CoordinatorControl::CreateSchema(uint64_t parent_schema_id,
                                     std::string schema_name,
                                     uint64_t& new_schema_id) {
  if (schema_map_.find(parent_schema_id) == schema_map_.end()) {
    LOG(INFO) << " CreateSchema parent_schema_id is illegal "
              << parent_schema_id;
    return -1;
  }

  if (schema_name.empty()) {
    LOG(INFO) << " CreateSchema schema_name is illegal " << schema_name;
    return -1;
  }

  new_schema_id = CreateSchemaId();
  schema_map_[parent_schema_id].add_schema_ids(new_schema_id);
  pb::common::Schema new_schema;
  new_schema.set_id(new_schema_id);
  new_schema.set_name(schema_name);

  schema_map_.insert(std::make_pair(new_schema_id, new_schema));

  return 0;
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

// GetSchemas
// in: schema_id
// out: schemas
void CoordinatorControl::GetSchemas(uint64_t schema_id,
                                    std::vector<pb::common::Schema>& schemas) {
  if (schema_id < 0) {
    LOG(ERROR) << "ERRROR: schema_id illegal " << schema_id;
    return;
  }

  if (!schemas.empty()) {
    LOG(ERROR) << "ERRROR: vector schemas is not empty , size="
               << schemas.size();
    return;
  }

  if (this->schema_map_.find(schema_id) == schema_map_.end()) {
    return;
  }

  auto& schema = schema_map_[schema_id];
  for (int i = 0; i < schema.schema_ids_size(); i++) {
    int sub_schema_id = schema.schema_ids(i);
    if (schema_map_.find(sub_schema_id) == schema_map_.end()) {
      LOG(ERROR) << "ERRROR: sub_schema_id " << sub_schema_id << " not exists";
      continue;
    }

    schemas.push_back(schema_map_[sub_schema_id]);
  }

  LOG(INFO) << "GetSchemas id=" << schema_id
            << " sub schema count=" << schema_map_.size();
}
}  // namespace dingodb