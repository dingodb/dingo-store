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

#ifndef DINGODB_META_CONTROL_H_
#define DINGODB_META_CONTROL_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

class MetaControl {
 public:
  virtual ~MetaControl() = default;
  MetaControl() = default;
  virtual bool Init() = 0;
  virtual bool IsLeader() = 0;
  virtual void SetLeader() = 0;
  virtual void SetNotLeader() = 0;

  virtual void GetLeaderLocation(pb::common::Location &leader_location) = 0;

  // set raft_node to coordinator_control
  virtual void SetRaftNode(std::shared_ptr<RaftNode> raft_node) = 0;

  // create region
  // in: resource_tag
  // out: new region id
  virtual int CreateRegion(const std::string &region_name, const std::string &resource_tag, int32_t replica_num,
                           pb::common::Range region_range, uint64_t schema_id, uint64_t table_id,
                           uint64_t &new_region_id, pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // drop region
  // in:  region_id
  // return: 0 or -1
  virtual int DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  // return: 0 or -1
  virtual int CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t &new_schema_id,
                           pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // create schema
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // return: 0 or -1
  virtual int CreateTable(uint64_t schema_id, const pb::meta::TableDefinition &table_definition, uint64_t &new_table_id,
                          pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // create store
  // in: cluster_id
  // out: store_id, password
  // return: 0 or -1
  virtual int CreateStore(uint64_t cluster_id, uint64_t &store_id, std::string &password,
                          pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // update store map with new Store info
  // return new epoch
  virtual uint64_t UpdateStoreMap(const pb::common::Store &store,
                                  pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // get storemap
  virtual void GetStoreMap(pb::common::StoreMap &store_map) = 0;

  // update region map with new Region info
  // return new epoch
  virtual uint64_t UpdateRegionMap(std::vector<pb::common::Region> &regions,
                                   pb::coordinator_internal::MetaIncrement &meta_increment) = 0;

  // get regionmap
  virtual void GetRegionMap(pb::common::RegionMap &region_map) = 0;

  // get schemas
  virtual void GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema> &schemas) = 0;

  // get tables
  virtual void GetTables(uint64_t schema_id,
                         std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids) = 0;

  // get table
  virtual void GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::Table &table) = 0;

  // get coordinator_map
  virtual void GetCoordinatorMap(uint64_t cluster_id, uint64_t &epoch, pb::common::Location &leader_location,
                                 std::vector<pb::common::Location> &locations) = 0;

  // on_apply callback
  // leader do need update next_xx_id, so leader call this function with update_ids=false
  virtual void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool update_ids) = 0;
};

}  // namespace dingodb

#endif  // DINGODB_META_CONTROL_H_
