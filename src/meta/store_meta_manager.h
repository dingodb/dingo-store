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

#include "butil/endpoint.h"
#include "engine/engine.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"

namespace dingodb {

// Manage store server store data
class StoreServerMeta {
 public:
  StoreServerMeta();
  ~StoreServerMeta() = default;

  bool Init();

  uint64_t GetEpoch() const;
  StoreServerMeta& SetEpoch(uint64_t epoch);
  StoreServerMeta& SetId(uint64_t id);
  StoreServerMeta& SetState(pb::common::StoreState state);
  StoreServerMeta& SetServerLocation(const butil::EndPoint&& endpoint);
  StoreServerMeta& SetRaftLocation(const butil::EndPoint&& endpoint);

  std::shared_ptr<pb::common::Store> GetStore();
  StoreServerMeta(const StoreServerMeta&) = delete;
  const StoreServerMeta& operator=(const StoreServerMeta&) = delete;

 private:
  uint64_t epoch_;
  std::shared_ptr<pb::common::Store> store_;
};

// Manage store server region meta data
class StoreRegionMeta : public TransformKvAble {
 public:
  StoreRegionMeta() : TransformKvAble("META_REGION"){};
  ~StoreRegionMeta() override = default;

  bool Init();
  bool Recover(const std::vector<pb::common::KeyValue>& kvs);

  uint64_t GetEpoch() const;
  bool IsExist(uint64_t region_id);

  void AddRegion(std::shared_ptr<pb::common::Region> region);
  void DeleteRegion(uint64_t region_id);
  std::shared_ptr<pb::common::Region> GetRegion(uint64_t region_id);
  std::map<uint64_t, std::shared_ptr<pb::common::Region> > GetAllRegion();

  uint64_t ParseRegionId(const std::string& str);
  std::string GenKey(uint64_t region_id) override;

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t region_id) override;
  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<pb::common::Region> region) override;
  std::vector<std::shared_ptr<pb::common::KeyValue> > TransformToKvtWithDelta() override;
  std::vector<std::shared_ptr<pb::common::KeyValue> > TransformToKvWithAll() override;

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) override;

  StoreRegionMeta(const StoreRegionMeta&) = delete;
  const StoreRegionMeta& operator=(const StoreRegionMeta&) = delete;

 private:
  uint64_t epoch_;

  // Record which region changed.
  std::vector<uint64_t> changed_regions_;
  // Protect regions_ concurrent access.
  std::shared_mutex mutex_;
  // Store all region meta data in this server.
  std::map<uint64_t, std::shared_ptr<pb::common::Region> > regions_;
};

// Manage store server meta data, like store and region.
// the data will report periodic.
class StoreMetaManager {
 public:
  StoreMetaManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer);
  ~StoreMetaManager() = default;

  bool Init();
  bool Recover();

  uint64_t GetServerEpoch();
  uint64_t GetRegionEpoch();

  std::shared_ptr<pb::common::Store> GetStoreServerMeta();

  bool IsExistRegion(uint64_t region_id);
  std::shared_ptr<pb::common::Region> GetRegion(uint64_t region_id);
  std::map<uint64_t, std::shared_ptr<pb::common::Region> > GetAllRegion();
  void AddRegion(std::shared_ptr<pb::common::Region> region);
  void DeleteRegion(uint64_t region_id);

  StoreMetaManager(const StoreMetaManager&) = delete;
  void operator=(const StoreMetaManager&) = delete;

 private:
  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // Store server meta data, like id/state/endpoint etc.
  std::unique_ptr<StoreServerMeta> server_meta_;
  // Store manage region meta data.
  std::unique_ptr<StoreRegionMeta> region_meta_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_META_MANAGER_H_