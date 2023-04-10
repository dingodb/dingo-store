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

#ifndef DINGODB_STORE_META_MANAGER_H_
#define DINGODB_STORE_META_MANAGER_H_

#include <memory>
#include <shared_mutex>
#include <vector>

#include "bthread/types.h"
#include "butil/endpoint.h"
#include "common/constant.h"
#include "engine/engine.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/store_internal.pb.h"

namespace dingodb {

// Manage store server store data
class StoreServerMeta {
 public:
  StoreServerMeta();
  ~StoreServerMeta();

  StoreServerMeta(const StoreServerMeta&) = delete;
  const StoreServerMeta& operator=(const StoreServerMeta&) = delete;

  bool Init();

  uint64_t GetEpoch() const;
  StoreServerMeta& SetEpoch(uint64_t epoch);

  bool IsExist(uint64_t store_id);

  void AddStore(std::shared_ptr<pb::common::Store> store);
  void UpdateStore(std::shared_ptr<pb::common::Store> store);
  void DeleteStore(uint64_t store_id);
  std::shared_ptr<pb::common::Store> GetStore(uint64_t store_id);
  std::map<uint64_t, std::shared_ptr<pb::common::Store>> GetAllStore();

 private:
  uint64_t epoch_;
  bthread_mutex_t mutex_;
  std::map<uint64_t, std::shared_ptr<pb::common::Store>> stores_;
};

// Manage store server region meta data
class StoreRegionMeta : public TransformKvAble {
 public:
  StoreRegionMeta();
  ~StoreRegionMeta() override;

  StoreRegionMeta(const StoreRegionMeta&) = delete;
  const StoreRegionMeta& operator=(const StoreRegionMeta&) = delete;

  bool Init();
  bool Recover(const std::vector<pb::common::KeyValue>& kvs);

  uint64_t GetEpoch() const;
  bool IsExist(uint64_t region_id);

  void AddRegion(std::shared_ptr<pb::common::Region> region);
  void DeleteRegion(uint64_t region_id);
  void UpdateRegion(std::shared_ptr<pb::common::Region> region);
  std::shared_ptr<pb::common::Region> GetRegion(uint64_t region_id);
  std::map<uint64_t, std::shared_ptr<pb::common::Region>> GetAllRegion();

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t region_id) override;
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<google::protobuf::Message> obj) override;
  std::vector<std::shared_ptr<pb::common::KeyValue>> TransformToKvtWithDelta() override;
  std::vector<std::shared_ptr<pb::common::KeyValue>> TransformToKvWithAll() override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

 private:
  uint64_t epoch_;

  // Record which region changed.
  std::vector<uint64_t> changed_regions_;
  // Protect regions_ concurrent access.
  bthread_mutex_t mutex_;
  // Store all region meta data in this server.
  std::map<uint64_t, std::shared_ptr<pb::common::Region>> regions_;
};

class StoreRaftMeta : public TransformKvAble {
 public:
  StoreRaftMeta();
  ~StoreRaftMeta() override;

  bool Init();
  bool Recover(const std::vector<pb::common::KeyValue>& kvs);

  void Add(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta);
  void Update(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta);
  void Delete(uint64_t region_id);
  std::shared_ptr<pb::store_internal::RaftMeta> Get(uint64_t region_id);

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t region_id) override;
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<google::protobuf::Message> obj) override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

 private:
  bthread_mutex_t mutex_;
  std::map<uint64_t, std::shared_ptr<pb::store_internal::RaftMeta>> raft_metas_;
};

// Manage store server meta data, like store and region.
// the data will report periodic.
class StoreMetaManager {
 public:
  StoreMetaManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer);
  ~StoreMetaManager();

  StoreMetaManager(const StoreMetaManager&) = delete;
  void operator=(const StoreMetaManager&) = delete;

  bool Init();
  bool Recover();

  uint64_t GetServerEpoch();
  uint64_t GetRegionEpoch();
  bthread_mutex_t* GetHeartbeatUpdateMutexRef();

  bool IsExistStore(uint64_t store_id);
  void AddStore(std::shared_ptr<pb::common::Store> store);
  void UpdateStore(std::shared_ptr<pb::common::Store> store);
  void DeleteStore(uint64_t store_id);
  std::shared_ptr<pb::common::Store> GetStore(uint64_t store_id);
  std::map<uint64_t, std::shared_ptr<pb::common::Store>> GetAllStore();

  bool IsExistRegion(uint64_t region_id);
  void AddRegion(std::shared_ptr<pb::common::Region> region);
  void UpdateRegion(std::shared_ptr<pb::common::Region> region);
  void DeleteRegion(uint64_t region_id);
  std::shared_ptr<pb::common::Region> GetRegion(uint64_t region_id);
  std::map<uint64_t, std::shared_ptr<pb::common::Region>> GetAllRegion();

  void AddRaftMeta(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta);
  void SaveRaftMeta(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta);
  void DeleteRaftMeta(uint64_t region_id);
  std::shared_ptr<pb::store_internal::RaftMeta> GetRaftMeta(uint64_t region_id);

 private:
  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // Store server meta data, like id/state/endpoint etc.
  std::unique_ptr<StoreServerMeta> server_meta_;
  // Store manage region meta data.
  std::unique_ptr<StoreRegionMeta> region_meta_;
  // Store raft meta.
  std::unique_ptr<StoreRaftMeta> raft_meta_;

  // heartbeat update mutex
  // only one heartbeat respone can be processed on time
  bthread_mutex_t heartbeat_update_mutex_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_META_MANAGER_H_