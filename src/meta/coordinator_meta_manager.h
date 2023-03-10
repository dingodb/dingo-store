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

#ifndef DINGODB_COORDINATOR_META_MANAGER_H_
#define DINGODB_COORDINATOR_META_MANAGER_H_

#include <memory>
#include <shared_mutex>
#include <vector>

#include "butil/endpoint.h"
#include "butil/strings/stringprintf.h"
#include "engine/engine.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "meta/transform_kv_able.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"

namespace dingodb {

// Manage coordinator server data
class CoordinatorServerMeta {
 public:
  CoordinatorServerMeta();
  ~CoordinatorServerMeta() = default;

  bool Init();

  uint64_t GetEpoch() const;
  CoordinatorServerMeta& SetEpoch(uint64_t epoch);
  CoordinatorServerMeta& SetId(uint64_t id);
  CoordinatorServerMeta& SetState(pb::common::CoordinatorState state);
  CoordinatorServerMeta& SetServerLocation(const butil::EndPoint&& endpoint);
  CoordinatorServerMeta& SetRaftLocation(const butil::EndPoint&& endpoint);

  std::shared_ptr<pb::common::Coordinator> GetCoordinator();
  CoordinatorServerMeta(const CoordinatorServerMeta&) = delete;
  const CoordinatorServerMeta& operator=(const CoordinatorServerMeta&) = delete;

 private:
  uint64_t epoch_;
  std::shared_ptr<pb::common::Coordinator> store_;
};

template <typename T>
class CoordinatorMap {
 public:
  const std::string internal_prefix;
  CoordinatorMap(const std::string& prefix) : internal_prefix(prefix){};
  CoordinatorMap() : internal_prefix(typeid(T).name()){};
  ~CoordinatorMap() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue>& kvs) {
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(uint64_t id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_.find(id);
    return static_cast<bool>(it != elements_.end());
  }

  uint64_t ParseId(const std::string& str) {
    if (str.size() <= internal_prefix.size()) {
      LOG(ERROR) << "Parse id failed, invalid str " << str;
      return 0;
    }

    std::string s(str.c_str() + internal_prefix.size() + 1);
    try {
      return std::stoull(s, nullptr, 10);
    } catch (std::invalid_argument& e) {
      LOG(ERROR) << "string to uint64_t failed: " << e.what();
    }

    return 0;
  }

  std::string GenKey(uint64_t region_id) { return butil::StringPrintf("%s_%lu", internal_prefix.c_str(), region_id); }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t id) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_.find(id);
    if (it == elements_.end()) {
      return nullptr;
    }

    return TransformToKv(it->second);
  };

  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::shared_ptr<T> element) {
    std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
    kv->set_key(GenKey(element->id()));
    kv->set_value(element->SerializeAsString());

    return kv;
  }

  std::vector<std::shared_ptr<pb::common::KeyValue>> TransformToKvWithAll() {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    std::vector<std::shared_ptr<pb::common::KeyValue>> kvs;
    for (const auto& it : elements_) {
      std::shared_ptr<pb::common::KeyValue> kv = std::make_shared<pb::common::KeyValue>();
      kv->set_key(GenKey(it.first));
      kv->set_value(it.second.SerializeAsString());
      kvs.push_back(kv);
    }

    return kvs;
  }

  void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (const auto& kv : kvs) {
      uint64_t id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      elements_.insert_or_assign(id, element);
    }
  };

  CoordinatorMap(const CoordinatorMap&) = delete;
  const CoordinatorMap& operator=(const CoordinatorMap&) = delete;

 private:
  // Protect regions_ concurrent access.
  std::shared_mutex mutex_;
  // Coordinator all region meta data in this server.
  std::map<uint64_t, T> elements_;
};

// Manage store server meta data, like store and region.
// the data will report periodic.
class CoordinatorMetaManager {
 public:
  CoordinatorMetaManager(std::shared_ptr<MetaReader> meta_reader, std::shared_ptr<MetaWriter> meta_writer);
  ~CoordinatorMetaManager() = default;

  bool Init();
  bool Recover();

  std::shared_ptr<pb::common::Coordinator> GetCoordinatorServerMeta();

  CoordinatorMetaManager(const CoordinatorMetaManager&) = delete;
  void operator=(const CoordinatorMetaManager&) = delete;

 private:
  // Read meta data from persistence storage.
  std::shared_ptr<MetaReader> meta_reader_;
  // Write meta data to persistence storage.
  std::shared_ptr<MetaWriter> meta_writer_;

  // Coordinator server meta data, like id/state/endpoint etc.
  std::unique_ptr<CoordinatorServerMeta> server_meta_;

  // Coordinator maps
  std::unique_ptr<CoordinatorMap<pb::coordinator_internal::CoordinatorInternal>> coordinator_meta_;
  std::unique_ptr<CoordinatorMap<pb::common::Store>> store_meta_;
  std::unique_ptr<CoordinatorMap<pb::meta::Schema>> schema_meta_;
  std::unique_ptr<CoordinatorMap<pb::common::Region>> region_meta_;
  std::unique_ptr<CoordinatorMap<pb::coordinator_internal::TableInternal>> table_meta_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_META_MANAGER_H_