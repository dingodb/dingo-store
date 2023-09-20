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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/endpoint.h"
#include "common/constant.h"
#include "common/safe_map.h"
#include "engine/engine.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/raft.pb.h"
#include "proto/store_internal.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

namespace store {

// Warp pb region for atomic/metux
class Region {
 public:
  Region() : split_strategy_(pb::raft::PRE_CREATE_REGION) { bthread_mutex_init(&mutex_, nullptr); };
  ~Region() { bthread_mutex_destroy(&mutex_); }

  Region(const Region&) = delete;
  void operator=(const Region&) = delete;

  static std::shared_ptr<Region> New();
  static std::shared_ptr<Region> New(const pb::common::RegionDefinition& definition);

  bool Recover();

  std::string Serialize();
  void DeSerialize(const std::string& data);

  uint64_t Id() const { return inner_region_.id(); }
  const std::string& Name() const { return inner_region_.definition().name(); }
  pb::common::RegionType Type() { return inner_region_.region_type(); }

  pb::common::RegionEpoch Epoch();
  void SetEpochVersion(uint64_t version);
  void SetEpochConfVersion(uint64_t version);

  uint64_t LeaderId();
  void SetLeaderId(uint64_t leader_id);

  const pb::common::Range& Range();
  void SetRange(const pb::common::Range& range);
  const pb::common::Range& RawRange();
  void SetRawRange(const pb::common::Range& range);
  // Get physics range, it's rocksdb data range.
  // store region raw_range == physics ragne
  // index region raw_range != physics ragne
  std::vector<pb::common::Range> PhysicsRange();

  std::string RangeToString();
  bool CheckKeyInRange(const std::string& key);

  void SetIndexParameter(const pb::common::IndexParameter& index_parameter);

  std::vector<pb::common::Peer> Peers();
  void SetPeers(std::vector<pb::common::Peer>& peers);

  pb::common::StoreRegionState State() const;
  void SetState(pb::common::StoreRegionState state);
  void AppendHistoryState(pb::common::StoreRegionState state) { inner_region_.add_history_states(state); }

  bool NeedBootstrapDoSnapshot();
  void SetNeedBootstrapDoSnapshot(bool need_do_snapshot);

  bool DisableChange();
  void SetDisableChange(bool disable_change);

  bool TemporaryDisableChange();
  void SetTemporaryDisableChange(bool disable_change);

  pb::raft::SplitStrategy SplitStrategy();
  void SetSplitStrategy(pb::raft::SplitStrategy split_strategy);

  uint64_t LastSplitTimestamp();
  void UpdateLastSplitTimestamp();

  uint64_t ParentId();
  void SetParentId(uint64_t region_id);

  std::vector<pb::store_internal::RegionSplitRecord> Childs();
  void AddChild(pb::store_internal::RegionSplitRecord& record);

  uint64_t PartitionId();

  const pb::store_internal::Region& InnerRegion() const { return inner_region_; }

  VectorIndexWrapperPtr VectorIndexWrapper() { return vector_index_wapper_; }
  void SetVectorIndexWrapper(VectorIndexWrapperPtr vector_index_wapper) { vector_index_wapper_ = vector_index_wapper; }

 private:
  bthread_mutex_t mutex_;
  pb::store_internal::Region inner_region_;
  std::atomic<pb::common::StoreRegionState> state_;

  pb::raft::SplitStrategy split_strategy_;

  VectorIndexWrapperPtr vector_index_wapper_;
};

using RegionPtr = std::shared_ptr<Region>;

}  // namespace store

// Manage store server store data
class StoreServerMeta {
 public:
  StoreServerMeta() { bthread_mutex_init(&mutex_, nullptr); }
  ~StoreServerMeta() { bthread_mutex_destroy(&mutex_); }

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
    regions_.Init(Constant::kStoreRegionMetaInitCapacity);
  }
  ~StoreRegionMeta() override = default;

  StoreRegionMeta(const StoreRegionMeta&) = delete;
  void operator=(const StoreRegionMeta&) = delete;

  bool Init();

  static uint64_t GetEpoch();

  void AddRegion(store::RegionPtr region);
  void DeleteRegion(uint64_t region_id);
  void UpdateRegion(store::RegionPtr region);

  void UpdateState(store::RegionPtr region, pb::common::StoreRegionState new_state);
  void UpdateState(uint64_t region_id, pb::common::StoreRegionState new_state);

  static void UpdateLeaderId(store::RegionPtr region, uint64_t leader_id);
  void UpdateLeaderId(uint64_t region_id, uint64_t leader_id);

  void UpdatePeers(store::RegionPtr region, std::vector<pb::common::Peer>& peers);
  void UpdatePeers(uint64_t region_id, std::vector<pb::common::Peer>& peers);

  void UpdateRange(store::RegionPtr region, const pb::common::Range& range);
  void UpdateRange(uint64_t region_id, const pb::common::Range& range);

  void UpdateEpochVersion(store::RegionPtr region, uint64_t version);
  void UpdateEpochVersion(uint64_t region_id, uint64_t version);
  void UpdateEpochConfVersion(store::RegionPtr region, uint64_t version);
  void UpdateEpochConfVersion(uint64_t region_id, uint64_t version);

  void UpdateNeedBootstrapDoSnapshot(store::RegionPtr region, bool need_do_snapshot);
  void UpdateDisableChange(store::RegionPtr region, bool disable_change);
  void UpdateTemporaryDisableChange(store::RegionPtr region, bool disable_change);

  bool IsExistRegion(uint64_t region_id);
  store::RegionPtr GetRegion(uint64_t region_id);
  std::vector<store::RegionPtr> GetAllRegion();
  std::vector<store::RegionPtr> GetAllAliveRegion();
  std::vector<store::RegionPtr> GetAllMetricsRegion();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // Store all region meta data in this server.
  using RegionMap = DingoSafeMap<uint64_t, store::RegionPtr>;
  RegionMap regions_;
};

class StoreRaftMeta : public TransformKvAble {
 public:
  StoreRaftMeta(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer)
      : TransformKvAble(Constant::kStoreRaftMetaPrefix), meta_reader_(meta_reader), meta_writer_(meta_writer) {
    bthread_mutex_init(&mutex_, nullptr);
  }
  ~StoreRaftMeta() override { bthread_mutex_destroy(&mutex_); }

  StoreRaftMeta(const StoreRaftMeta&) = delete;
  void operator=(const StoreRaftMeta&) = delete;

  using RaftMetaPtr = std::shared_ptr<pb::store_internal::RaftMeta>;

  bool Init();

  static RaftMetaPtr NewRaftMeta(uint64_t region_id);

  void AddRaftMeta(RaftMetaPtr raft_meta);
  void UpdateRaftMeta(RaftMetaPtr raft_meta);
  void DeleteRaftMeta(uint64_t region_id);
  RaftMetaPtr GetRaftMeta(uint64_t region_id);
  std::vector<RaftMetaPtr> GetAllRaftMeta();

 private:
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any obj) override;
  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  bthread_mutex_t mutex_;

  using RaftMetaMap = std::map<uint64_t, RaftMetaPtr>;
  RaftMetaMap raft_metas_;
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