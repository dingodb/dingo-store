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
#include "proto/common.pb.h"

namespace dingodb {

class KvTransformAble {
 public:
  KvTransformAble(){};
  ~KvTransformAble(){};

  virtual std::shared_ptr<pb::common::KeyValue> Transform() = 0;
  virtual std::vector<pb::common::KeyValue> TransformAll() = 0;
  virtual std::vector<pb::common::KeyValue> TransformDelta() = 0;

 protected:
  std::string prefix_;
};

// Manage store server store data
class StoreServerMeta {
 public:
  StoreServerMeta();
  ~StoreServerMeta();

  uint64_t GetEpoch();
  StoreServerMeta& SetEpoch(uint64_t epoch);
  StoreServerMeta& SetId(uint64_t id);
  StoreServerMeta& SetStatus(pb::common::StoreStatus status);
  StoreServerMeta& SetServerLocation(const butil::EndPoint&& endpoint);
  StoreServerMeta& SetRaftLocation(const butil::EndPoint&& endpoint);

  std::shared_ptr<pb::common::Store> GetStore();

 private:
  uint64_t epoch_;
  std::shared_ptr<pb::common::Store> store_;
  std::map<uint64_t, pb::common::Store> history_stores_;
};

// Manage store server region meta data
class StoreRegionMeta : public KvTransformAble {
 public:
  StoreRegionMeta();
  ~StoreRegionMeta();

  uint64_t GetEpoch();
  bool IsExist(uint64_t region_id);
  void AddRegion(uint64_t region_id, const pb::common::Region& region);
  std::shared_ptr<pb::common::Region> GetRegion(uint64_t region_id);
  std::vector<std::shared_ptr<pb::common::Region> > GetAllRegion();

  std::shared_ptr<pb::common::KeyValue> Transform();
  std::vector<pb::common::KeyValue> TransformAll();
  std::vector<pb::common::KeyValue> TransformDelta();

 private:
  uint64_t epoch_;
  std::shared_mutex mutex_;
  std::map<uint64_t, std::shared_ptr<pb::common::Region> > regions_;
};

// Manage store server meta data, like store and region.
// the data will report periodic.
class StoreMetaManager {
 public:
  StoreMetaManager();
  ~StoreMetaManager();

  uint64_t GetServerEpoch();
  uint64_t GetRegionEpoch();
  std::shared_ptr<pb::common::Store> GetStore();
  std::vector<std::shared_ptr<pb::common::Region> > GetAllRegion();
  void AddRegion(uint64_t region_id, const pb::common::Region& region);

 private:
  DISALLOW_COPY_AND_ASSIGN(StoreMetaManager);

  std::unique_ptr<StoreServerMeta> server_meta_;
  std::unique_ptr<StoreRegionMeta> region_meta_;
};

}  // namespace dingodb

#endif  // DINGODB_STORE_META_MANAGER_H_