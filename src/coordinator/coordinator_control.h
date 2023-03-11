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

#ifndef DINGODB_COORDINATOR_CONTROL_H_
#define DINGODB_COORDINATOR_CONTROL_H_

#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "butil/strings/stringprintf.h"
#include "common/meta_control.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "proto/meta.pb.h"
#include "raft/raft_node.h"

namespace dingodb {

template <typename T>
class MetaMapStorage {
 public:
  const std::string internal_prefix;
  MetaMapStorage(std::map<uint64_t, T> *elements_ptr) : internal_prefix(typeid(T).name()), elements_(elements_ptr){};
  MetaMapStorage(std::map<uint64_t, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(prefix), elements_(elements_ptr){};
  ~MetaMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(uint64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    return static_cast<bool>(it != elements_->end());
  }

  uint64_t ParseId(const std::string &str) {
    if (str.size() <= internal_prefix.size()) {
      LOG(ERROR) << "Parse id failed, invalid str " << str;
      return 0;
    }

    std::string s(str.c_str() + internal_prefix.size() + 1);
    try {
      return std::stoull(s, nullptr, 10);
    } catch (std::invalid_argument &e) {
      LOG(ERROR) << "string to uint64_t failed: " << e.what();
    }

    return 0;
  }

  std::string GenKey(uint64_t region_id) { return butil::StringPrintf("%s_%lu", internal_prefix.c_str(), region_id); }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    if (it == elements_->end()) {
      return nullptr;
    }

    return TransformToKv(it->second);
  };

  std::shared_ptr<pb::common::KeyValue> TransformToKv(T element) {
    std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
    kv->set_key(GenKey(element.id()));
    kv->set_value(element.SerializeAsString());

    return kv;
  }

  pb::common::KeyValue TransformToKvValue(T element) {
    pb::common::KeyValue kv;
    kv.set_key(GenKey(element.id()));
    kv.set_value(element.SerializeAsString());

    return kv;
  }

  std::vector<pb::common::KeyValue> TransformToKvWithAll() {
    // std::shared_lock<std::shared_mutex> lock(mutex_);

    std::vector<pb::common::KeyValue> kvs;
    for (const auto &it : *elements_) {
      pb::common::KeyValue kv;
      kv.set_key(GenKey(it.first));
      kv.set_value(it.second.SerializeAsString());
      kvs.push_back(kv);
    }

    return kvs;
  }

  void TransformFromKv(const std::vector<pb::common::KeyValue> &kvs) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    for (const auto &kv : kvs) {
      uint64_t id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      elements_->insert_or_assign(id, element);
    }
  };

  MetaMapStorage(const MetaMapStorage &) = delete;
  const MetaMapStorage &operator=(const MetaMapStorage &) = delete;

 private:
  // Protect regions_ concurrent access.
  // std::shared_mutex mutex_;
  // Coordinator all region meta data in this server.
  // std::map<uint64_t, std::shared_ptr<T>> *elements_;
  std::map<uint64_t, T> *elements_;
};

class CoordinatorControl : public MetaControl {
 public:
  CoordinatorControl(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer);
  ~CoordinatorControl() override;
  bool Recover();
  static void GenerateRootSchemas(pb::coordinator_internal::SchemaInternal &root_schema,
                                  pb::coordinator_internal::SchemaInternal &meta_schema,
                                  pb::coordinator_internal::SchemaInternal &dingo_schema);
  // static void GenerateRootSchemasMetaIncrement(pb::coordinator_internal::SchemaInternal &root_schema,
  //                                              pb::coordinator_internal::SchemaInternal &meta_schema,
  //                                              pb::coordinator_internal::SchemaInternal &dingo_schema,
  //                                              pb::coordinator_internal::MetaIncrement &meta_increment);
  bool Init() override;
  bool IsLeader() override;
  void SetLeader() override;
  void SetNotLeader() override;

  void GetLeaderLocation(pb::common::Location &leader_location) override;

  // create region
  // in: resource_tag
  // out: new region id
  int CreateRegion(const std::string &region_name, const std::string &resource_tag, int32_t replica_num,
                   pb::common::Range region_range, uint64_t schema_id, uint64_t table_id, uint64_t &new_region_id,
                   pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // drop region
  // in:  region_id
  // return: 0 or -1
  int DropRegion(uint64_t region_id, pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // create schema
  // in: parent_schema_id
  // in: schema_name
  // out: new schema_id
  // return: 0 or -1
  int CreateSchema(uint64_t parent_schema_id, std::string schema_name, uint64_t &new_schema_id,
                   pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // create schema
  // in: schema_id
  // in: table_definition
  // out: new table_id
  // return: 0 or -1
  int CreateTable(uint64_t schema_id, const pb::meta::TableDefinition &table_definition, uint64_t &new_table_id,
                  pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // create store
  // in: cluster_id
  // out: store_id, password
  // return: 0 or -1
  int CreateStore(uint64_t cluster_id, uint64_t &store_id, std::string &password,
                  pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // update store map with new Store info
  // return new epoch
  uint64_t UpdateStoreMap(const pb::common::Store &store,
                          pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // get storemap
  void GetStoreMap(pb::common::StoreMap &store_map) override;

  // update region map with new Region info
  // return new epoch
  uint64_t UpdateRegionMap(std::vector<pb::common::Region> &regions,
                           pb::coordinator_internal::MetaIncrement &meta_increment) override;

  // get regionmap
  void GetRegionMap(pb::common::RegionMap &region_map) override;

  // get schemas
  void GetSchemas(uint64_t schema_id, std::vector<pb::meta::Schema> &schemas) override;

  // get tables
  void GetTables(uint64_t schema_id, std::vector<pb::meta::TableDefinitionWithId> &table_definition_with_ids) override;

  // get table
  // in: schema_id
  // in: table_id
  // out: Table
  void GetTable(uint64_t schema_id, uint64_t table_id, pb::meta::Table &table) override;

  // drop table
  // in: schema_id
  // in: table_id
  // out: meta_increment
  // return: 0 or -1
  int DropTable(uint64_t schema_id, uint64_t table_id, pb::coordinator_internal::MetaIncrement &meta_increment);

  // get coordinator_map
  void GetCoordinatorMap(uint64_t cluster_id, uint64_t &epoch, pb::common::Location &leader_location,
                         std::vector<pb::common::Location> &locations) override;

  // on_apply callback
  // leader do need update next_xx_id, so leader call this function with update_ids=false
  void ApplyMetaIncrement(pb::coordinator_internal::MetaIncrement &meta_increment, bool update_ids) override;

  // get next id/epoch
  uint64_t GetNextId(const pb::coordinator_internal::IdEpochType &key,
                     pb::coordinator_internal::MetaIncrement &meta_increment);

  // get present id/epoch
  uint64_t GetPresentId(const pb::coordinator_internal::IdEpochType &key);

  // set raft_node to coordinator_control
  void SetRaftNode(std::shared_ptr<RaftNode> raft_node) override;

 private:
  // mutex
  bthread_mutex_t control_mutex_;

  // // global ids
  // uint64_t next_coordinator_id_;
  // uint64_t next_store_id_;
  // uint64_t next_schema_id_;
  // uint64_t next_region_id_;
  // uint64_t next_table_id_;

  // coordinators
  std::map<uint64_t, pb::coordinator_internal::CoordinatorInternal> coordinator_map_;
  MetaMapStorage<pb::coordinator_internal::CoordinatorInternal> *coordinator_meta_;

  // stores
  std::map<uint64_t, pb::common::Store> store_map_;
  MetaMapStorage<pb::common::Store> *store_meta_;

  // schemas
  std::map<uint64_t, pb::coordinator_internal::SchemaInternal> schema_map_;
  MetaMapStorage<pb::coordinator_internal::SchemaInternal> *schema_meta_;

  // regions
  std::map<uint64_t, pb::common::Region> region_map_;
  MetaMapStorage<pb::common::Region> *region_meta_;

  // tables
  // TableInternal is combination of Table & TableDefinition
  std::map<uint64_t, pb::coordinator_internal::TableInternal> table_map_;
  MetaMapStorage<pb::coordinator_internal::TableInternal> *table_meta_;

  // ids_epochs
  // TableInternal is combination of Table & TableDefinition
  std::map<uint64_t, pb::coordinator_internal::IdEpochInternal> id_epoch_map_;
  MetaMapStorage<pb::coordinator_internal::IdEpochInternal> *id_epoch_meta_;

  // root schema write to raft
  bool root_schema_writed_to_raft_;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // node is leader or not
  std::atomic<bool> is_leader_;

  // raft node
  std::shared_ptr<RaftNode> raft_node_;

  uint64_t test_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_CONTROL_H_
