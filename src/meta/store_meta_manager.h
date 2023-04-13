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

#include <cstdint>
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
  StoreRegionMeta(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRegionMetaPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~StoreRegionMeta() override { bthread_mutex_destroy(&mutex_); }

  bool Init();

  uint64_t GetEpoch();

  bool IsExistRegion(uint64_t region_id);
  void AddRegion(std::shared_ptr<pb::store_internal::Region> region);
  void DeleteRegion(uint64_t region_id);
  void UpdateRegion(std::shared_ptr<pb::store_internal::Region> region);

  void UpdateState(std::shared_ptr<pb::store_internal::Region> region, pb::common::StoreRegionState new_state);
  void UpdateState(uint64_t region_id, pb::common::StoreRegionState new_state);

  void UpdateLeaderId(std::shared_ptr<pb::store_internal::Region> region, uint64_t leader_id);
  void UpdateLeaderId(uint64_t region_id, uint64_t leader_id);

  std::shared_ptr<pb::store_internal::Region> GetRegion(uint64_t region_id);
  std::vector<std::shared_ptr<pb::store_internal::Region>> GetAllRegion();
  std::vector<std::shared_ptr<pb::store_internal::Region>> GetAllAliveRegion();

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t region_id) override;
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<google::protobuf::Message> obj) override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

 private:
  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;
  // Store all region meta data in this server.
  // Todo: use class wrap pb::store_internal::Region for mutex
  std::map<uint64_t, std::shared_ptr<pb::store_internal::Region>> regions_;
};

class StoreRaftMeta : public TransformKvAble {
 public:
  StoreRaftMeta(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRaftMetaPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~StoreRaftMeta() override { bthread_mutex_destroy(&mutex_); }

  bool Init();

  static std::shared_ptr<pb::store_internal::RaftMeta> NewRaftMeta(uint64_t region_id);

  void AddRaftMeta(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta);
  void UpdateRaftMeta(std::shared_ptr<pb::store_internal::RaftMeta> raft_meta);
  void DeleteRaftMeta(uint64_t region_id);
  std::shared_ptr<pb::store_internal::RaftMeta> GetRaftMeta(uint64_t region_id);

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t region_id) override;
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<google::protobuf::Message> obj) override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

 private:
  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;
  std::map<uint64_t, std::shared_ptr<pb::store_internal::RaftMeta>> raft_metas_;
};

// Manage store server meta data, like store and region.
// the data will report periodic.
class StoreMetaManager {
 public:
  StoreMetaManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : server_meta_(std::make_shared<StoreServerMeta>()),
        region_meta_(std::make_shared<StoreRegionMeta>(meta_reader, meta_writer)),
        raft_meta_(std::make_shared<StoreRaftMeta>(meta_reader, meta_writer)) {}
  ~StoreMetaManager() = default;

  StoreMetaManager(const StoreMetaManager&) = delete;
  void operator=(const StoreMetaManager&) = delete;

  bool Init();

  std::shared_ptr<StoreServerMeta> GetStoreServerMeta() { return server_meta_; }
  std::shared_ptr<StoreRegionMeta> GetStoreRegionMeta() { return region_meta_; }
  std::shared_ptr<StoreRaftMeta> GetStoreRaftMeta() { return raft_meta_; }

 private:
  // Store server meta data, like id/state/endpoint etc.
  std::shared_ptr<StoreServerMeta> server_meta_;
  // Store manage region meta data.
  std::shared_ptr<StoreRegionMeta> region_meta_;
  // Store raft meta.
  std::shared_ptr<StoreRaftMeta> raft_meta_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_META_MANAGER_H_