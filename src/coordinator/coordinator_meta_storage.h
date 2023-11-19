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

#ifndef DINGODB_COORDINATOR_META_STORAGE_H_
#define DINGODB_COORDINATOR_META_STORAGE_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "butil/containers/doubly_buffered_data.h"
#include "butil/containers/flat_map.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/safe_map.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"
#include "raft/raft_node.h"
#include "serial/buf.h"
#include "serial/schema/dingo_schema.h"
#include "serial/schema/long_schema.h"

namespace dingodb {

#define COORDINATOR_ID_OF_MAP_MIN 1000

// Implement a ThreadSafeMap
// This is for IdEpoch with atomic GetNextId function
// all membber functions return 1 if success, return -1 if failed
// all inner functions return 0 if success, return -1 if failed
using DingoSafeIdEpochMapBase = DingoSafeMap<int64_t, pb::coordinator_internal::IdEpochInternal>;
class DingoSafeIdEpochMap : public DingoSafeIdEpochMapBase {
 public:
  using TypeFlatMap = butil::FlatMap<int64_t, pb::coordinator_internal::IdEpochInternal>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeFlatMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  int GetPresentId(const int64_t &key, int64_t &value) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    auto *value_ptr = ptr->seek(key);
    if (!value_ptr) {
      // if not exist, return 0
      value = 0;
      return 1;
    } else {
      value = value_ptr->value();
    }

    return 1;
  }

  int64_t GetPresentId(const int64_t &key) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    auto *value_ptr = ptr->seek(key);
    if (!value_ptr) {
      // if not exist, return 0
      return 0;
    } else {
      return value_ptr->value();
    }

    return 0;
  }

  int GetNextId(const int64_t &key, int64_t &value) {
    if (safe_map.Modify(InnerGetNextId, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  int GetNextIds(const int64_t &key, const int64_t &count, std::vector<int64_t> &values) {
    if (count <= 0 || key <= 0) {
      return -1;
    }

    std::pair<int64_t, int64_t> key_and_count{key, count};
    if (safe_map.Modify(InnerGetNextIds, key_and_count, values) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  int UpdatePresentId(const int64_t &key, const int64_t &value) {
    if (safe_map.Modify(InnerUpdatePresentId, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

 private:
  static size_t InnerGetNextId(TypeFlatMap &map, const int64_t &key, const int64_t &value) {
    // Notice: The brpc's template restrict to return value in Modify process, but we need to do this, so use a
    // const_cast to modify the input parameter here
    auto &value_mod = const_cast<int64_t &>(value);

    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      // if not exist, construct a new value and return
      pb::coordinator_internal::IdEpochInternal new_value;
      new_value.set_id(key);
      new_value.set_value(COORDINATOR_ID_OF_MAP_MIN + 1);
      map.insert(key, new_value);

      value_mod = new_value.value();
    } else {
      value_ptr->set_value(value_ptr->value() + 1);

      value_mod = value_ptr->value();
    }

    return 1;
  }

  static size_t InnerGetNextIds(TypeFlatMap &map, const std::pair<int64_t, int64_t> &key_and_count,
                                const std::vector<int64_t> &values) {
    // Notice: The brpc's template restrict to return value in Modify process, but we need to do this, so use a
    // const_cast to modify the input parameter here
    auto &values_mod = const_cast<std::vector<int64_t> &>(values);

    bool need_insert = false;
    if (values_mod.empty()) {
      need_insert = true;
    }

    auto *value_ptr = map.seek(key_and_count.first);
    if (value_ptr == nullptr) {
      if (need_insert) {
        for (int64_t i = 0; i < key_and_count.second; i++) {
          values_mod.push_back(COORDINATOR_ID_OF_MAP_MIN + i + 1);
        }
      }

      // if not exist, construct a new value and return
      pb::coordinator_internal::IdEpochInternal new_value;
      new_value.set_id(key_and_count.first);
      new_value.set_value(COORDINATOR_ID_OF_MAP_MIN + key_and_count.second);
      map.insert(key_and_count.first, new_value);

    } else {
      if (need_insert) {
        for (int64_t i = 0; i < key_and_count.second; i++) {
          values_mod.push_back(value_ptr->value() + i + 1);
        }
      }

      value_ptr->set_value(value_ptr->value() + key_and_count.second);
    }

    return 1;
  }

  static size_t InnerUpdatePresentId(TypeFlatMap &map, const int64_t &key, const int64_t &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      // if not exist, construct a new internal with input value
      pb::coordinator_internal::IdEpochInternal new_value;
      new_value.set_id(key);
      new_value.set_value(value);
      map.insert(key, new_value);

      return 1;
    } else if (value_ptr->value() < value) {
      // if exist, update the value if input value is larger than the value in map
      value_ptr->set_value(value);

      return 1;
    } else {
      return 0;
    }
  }
};

using DingoSafeSchemaMapBase = DingoSafeMap<int64_t, pb::coordinator_internal::SchemaInternal>;
class DingoSafeSchemaMap : public DingoSafeIdEpochMapBase {
 public:
  using TypeFlatMap = butil::FlatMap<int64_t, pb::coordinator_internal::IdEpochInternal>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeFlatMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  int GetPresentId(const int64_t &key, int64_t &value) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    auto *value_ptr = ptr->seek(key);
    if (!value_ptr) {
      // if not exist, return 0
      value = 0;
      return 1;
    } else {
      value = value_ptr->value();
    }

    return 1;
  }

  int GetNextId(const int64_t &key, int64_t &value) {
    if (safe_map.Modify(InnerGetNextId, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  int UpdatePresentId(const int64_t &key, const int64_t &value) {
    if (safe_map.Modify(InnerUpdatePresentId, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Erase
  // erase key-value pair from map
  // int Erase(const int64_t &key) {
  //   return DingoSafeMap<int64_t, pb::coordinator_internal::IdEpochInternal>::Erase(key);
  // }

 private:
  static size_t InnerGetNextId(TypeFlatMap &map, const int64_t &key, const int64_t &value) {
    // Notice: The brpc's template restrict to return value in Modify process, but we need to do this, so use a
    // const_cast to modify the input parameter here
    auto &value_mod = const_cast<int64_t &>(value);

    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      // if not exist, construct a new value and return
      pb::coordinator_internal::IdEpochInternal new_value;
      new_value.set_id(key);
      new_value.set_value(COORDINATOR_ID_OF_MAP_MIN + 1);
      map.insert(key, new_value);

      value_mod = new_value.value();
    } else {
      value_ptr->set_value(value_ptr->value() + 1);

      value_mod = value_ptr->value();
    }

    return 1;
  }

  static size_t InnerUpdatePresentId(TypeFlatMap &map, const int64_t &key, const int64_t &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      // if not exist, construct a new internal with input value
      pb::coordinator_internal::IdEpochInternal new_value;
      new_value.set_id(key);
      new_value.set_value(value);
      map.insert(key, new_value);

      return 1;
    } else if (value_ptr->value() < value) {
      // if exist, update the value if input value is larger than the value in map
      value_ptr->set_value(value_ptr->value());

      return 1;
    } else {
      return 0;
    }
  }
};

// MetaMapStorage is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaSafeMapStorage {
 public:
  const std::string internal_prefix;
  MetaSafeMapStorage(DingoSafeMap<int64_t, T> *elements_ptr)
      : internal_prefix(std::string("meta_map_safe") + typeid(T).name()), elements_(elements_ptr){};
  MetaSafeMapStorage(DingoSafeMap<int64_t, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(std::string("meta_map_safe") + prefix), elements_(elements_ptr){};
  ~MetaSafeMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    elements_->Clear();
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(int64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    return static_cast<bool>(it != elements_->end());
  }

  int64_t ParseId(const std::string &str) {
    if (str.size() <= internal_prefix.size()) {
      DINGO_LOG(ERROR) << "Parse id failed, invalid str:[" << str << "], prefix:[" << internal_prefix << "]";
      return 0;
    }

    // std::string s(str.c_str() + internal_prefix.size() + 1);
    std::string s = str.substr(internal_prefix.size() + 1);
    try {
      return std::stoll(s, nullptr, 10);
    } catch (std::invalid_argument &e) {
      DINGO_LOG(ERROR) << "string to int64_t failed: " << e.what();
    }

    return 0;
  }

  // std::string GenKey(int64_t id) { return fmt::format("{}_{}", internal_prefix, id); }
  std::string GenKey(int64_t id) { return internal_prefix + "_" + std::to_string(id); }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(int64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    T value;
    if (elements_->Get(id, value) < 0) {
      return nullptr;
    }

    return TransformToKv(value);
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
    butil::FlatMap<int64_t, T> temp_map;
    temp_map.init(10000);
    elements_->GetRawMapCopy(temp_map);

    std::vector<pb::common::KeyValue> kvs;
    for (const auto &it : temp_map) {
      pb::common::KeyValue kv;
      kv.set_key(GenKey(it.first));
      kv.set_value(it.second.SerializeAsString());
      kvs.push_back(kv);
    }

    return kvs;
  }

  void TransformFromKv(const std::vector<pb::common::KeyValue> &kvs) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<int64_t> key_list;
    std::vector<T> value_list;

    for (const auto &kv : kvs) {
      int64_t id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      // elements_->insert_or_assign(id, element);
      // elements_->insert(id, element);
      key_list.push_back(id);
      value_list.push_back(element);
    }

    elements_->MultiPut(key_list, value_list);
  };

  MetaSafeMapStorage(const MetaSafeMapStorage &) = delete;
  const MetaSafeMapStorage &operator=(const MetaSafeMapStorage &) = delete;

 private:
  // Coordinator all region meta data in this server.
  // std::map<int64_t, std::shared_ptr<T>> *elements_;
  DingoSafeMap<int64_t, T> *elements_;
};

// MetaSafeStringMapStorage is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaSafeStringMapStorage {
 public:
  const std::string internal_prefix;
  MetaSafeStringMapStorage(DingoSafeMap<std::string, T> *elements_ptr)
      : internal_prefix(std::string("meta_map_safe_string") + typeid(T).name()), elements_(elements_ptr){};
  MetaSafeStringMapStorage(DingoSafeMap<std::string, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(std::string("meta_map_safe_string") + prefix), elements_(elements_ptr){};
  ~MetaSafeStringMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    elements_->Clear();
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(std::string id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    return static_cast<bool>(it != elements_->end());
  }

  std::string ParseId(const std::string &str) {
    if (str.size() <= internal_prefix.size()) {
      DINGO_LOG(ERROR) << "Parse id failed, invalid str " << str;
      return std::string();
    }

    // std::string s(str.c_str() + internal_prefix.size() + 1);
    std::string s = str.substr(internal_prefix.size() + 1);
    return s;
  }

  // std::string GenKey(std::string id) { return fmt::format("{}_{}", internal_prefix, id); }
  std::string GenKey(std::string id) { return internal_prefix + "_" + id; }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::string id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    T value;
    if (elements_->Get(id, value) < 0) {
      return nullptr;
    }

    return TransformToKv(value);
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
    butil::FlatMap<std::string, T> temp_map;
    temp_map.init(10000);
    elements_->GetRawMapCopy(temp_map);

    std::vector<pb::common::KeyValue> kvs;
    for (const auto &it : temp_map) {
      pb::common::KeyValue kv;
      kv.set_key(GenKey(it.first));
      kv.set_value(it.second.SerializeAsString());
      kvs.push_back(kv);
    }

    return kvs;
  }

  void TransformFromKv(const std::vector<pb::common::KeyValue> &kvs) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<std::string> key_list;
    std::vector<T> value_list;

    for (const auto &kv : kvs) {
      std::string id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      // elements_->insert_or_assign(id, element);
      // elements_->insert(id, element);
      key_list.push_back(id);
      value_list.push_back(element);
    }

    elements_->MultiPut(key_list, value_list);
  };

  MetaSafeStringMapStorage(const MetaSafeStringMapStorage &) = delete;
  const MetaSafeStringMapStorage &operator=(const MetaSafeStringMapStorage &) = delete;

 private:
  // Coordinator all region meta data in this server.
  DingoSafeMap<std::string, T> *elements_;
};

// MetaMapStorage is a meta storage based on map.
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaMapStorage {
 public:
  const std::string internal_prefix;
  MetaMapStorage(butil::FlatMap<int64_t, T> *elements_ptr)
      : internal_prefix(std::string("meta_map_flat") + typeid(T).name()), elements_(elements_ptr){};
  MetaMapStorage(butil::FlatMap<int64_t, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(std::string("meta_map_flat") + prefix), elements_(elements_ptr){};
  ~MetaMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    elements_->clear();
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(int64_t id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    return static_cast<bool>(it != elements_->end());
  }

  int64_t ParseId(const std::string &str) {
    if (str.size() <= internal_prefix.size()) {
      DINGO_LOG(ERROR) << "Parse id failed, invalid str " << str;
      return 0;
    }

    // std::string s(str.c_str() + internal_prefix.size() + 1);
    std::string s = str.substr(internal_prefix.size() + 1);
    try {
      return std::stoll(s, nullptr, 10);
    } catch (std::invalid_argument &e) {
      DINGO_LOG(ERROR) << "string to int64_t failed: " << e.what();
    }

    return 0;
  }

  // std::string GenKey(int64_t id) { return fmt::format("{}_{}", internal_prefix, id); }
  std::string GenKey(int64_t id) { return internal_prefix + "_" + std::to_string(id); }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(int64_t id) {
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
      int64_t id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      // elements_->insert_or_assign(id, element);
      elements_->insert(id, element);
    }
  };

  MetaMapStorage(const MetaMapStorage &) = delete;
  const MetaMapStorage &operator=(const MetaMapStorage &) = delete;

 private:
  // Coordinator all region meta data in this server.
  // std::map<int64_t, std::shared_ptr<T>> *elements_;
  butil::FlatMap<int64_t, T> *elements_;
};

// MetaSafeStringStdMapStorage is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaSafeStringStdMapStorage {
 public:
  const std::string internal_prefix;
  MetaSafeStringStdMapStorage(DingoSafeStdMap<std::string, T> *elements_ptr)
      : internal_prefix(std::string("meta_stdmap_safe_string") + typeid(T).name()), elements_(elements_ptr){};
  MetaSafeStringStdMapStorage(DingoSafeStdMap<std::string, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(std::string("meta_stdmap_safe_string") + prefix), elements_(elements_ptr){};
  ~MetaSafeStringStdMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
    return true;
  }

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    elements_->Clear();
    TransformFromKv(kvs);
    return true;
  }

  bool IsExist(std::string id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = elements_->find(id);
    return static_cast<bool>(it != elements_->end());
  }

  std::string ParseId(const std::string &str) {
    if (str.size() <= internal_prefix.size()) {
      DINGO_LOG(ERROR) << "Parse id failed, invalid str " << str;
      return std::string();
    }

    return str.substr(internal_prefix.size() + 1);

    // std::string s(str.c_str() + internal_prefix.size() + 1);
    // return s;
  }

  // std::string GenKey(std::string id) { return fmt::format("{}_{}", internal_prefix, id); }
  std::string GenKey(std::string id) { return internal_prefix + "_" + id; }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(std::string id) {
    // std::shared_lock<std::shared_mutex> lock(mutex_);
    T value;
    if (elements_->Get(id, value) < 0) {
      return nullptr;
    }

    return TransformToKv(value);
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
    butil::FlatMap<std::string, T> temp_map;
    temp_map.init(10000);
    elements_->GetRawMapCopy(temp_map);

    std::vector<pb::common::KeyValue> kvs;
    for (const auto &it : temp_map) {
      pb::common::KeyValue kv;
      kv.set_key(GenKey(it.first));
      kv.set_value(it.second.SerializeAsString());
      kvs.push_back(kv);
    }

    return kvs;
  }

  void TransformFromKv(const std::vector<pb::common::KeyValue> &kvs) {
    // std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<std::string> key_list;
    std::vector<T> value_list;

    for (const auto &kv : kvs) {
      std::string id = ParseId(kv.key());
      T element;
      element.ParsePartialFromArray(kv.value().data(), kv.value().size());
      // elements_->insert_or_assign(id, element);
      // elements_->insert(id, element);
      key_list.push_back(id);
      value_list.push_back(element);
    }

    elements_->MultiPut(key_list, value_list);
  };

  MetaSafeStringStdMapStorage(const MetaSafeStringStdMapStorage &) = delete;
  const MetaSafeStringStdMapStorage &operator=(const MetaSafeStringStdMapStorage &) = delete;

 private:
  // Coordinator all region meta data in this server.
  DingoSafeStdMap<std::string, T> *elements_;
};

// MetaMapStr is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaMap {
 public:
  const std::string internal_prefix;
  MetaMap(const std::string &prefix, std::shared_ptr<RawEngine> raw_engine)
      : internal_prefix(std::string("META_MAP_") + prefix), raw_engine_(raw_engine){};
  ~MetaMap() = default;

  std::string Prefix() { return internal_prefix; }

  std::string ParseId(const std::string &key) {
    if (key.size() <= internal_prefix.size()) {
      DINGO_LOG(ERROR) << "Parse id failed, invalid str " << key;
      return std::string();
    }

    return key.substr(internal_prefix.size() + 1);
  }

  int64_t ParseIntId(const std::string &key) {
    auto str_id = ParseId(key);
    if (str_id.size() != sizeof(int64_t)) {
      return 0;
    }

    return DecodeInt64(str_id);
  }

  static std::string EncodeInt64(int64_t id) {
    Buf buf(sizeof(int64_t));
    DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, id);
    return buf.GetString();
  }

  int64_t DecodeInt64(const std::string &str_id) {
    CHECK(str_id.size() == sizeof(int64_t));

    Buf buf(str_id);

    return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
  }

  std::string GenKey(std::string id) { return internal_prefix + "_" + id; }
  std::string GenKey(int64_t id) { return internal_prefix + "_" + EncodeInt64(id); }

  pb::common::KeyValue TransformToKvValue(T element) {
    pb::common::KeyValue kv;
    kv.set_key(GenKey(element.id()));
    kv.set_value(element.SerializeAsString());
    return kv;
  }

  void TransformToKvValue(T element, pb::common::KeyValue &kv) {
    kv.set_key(GenKey(element.id()));
    kv.set_value(element.SerializeAsString());
  }

  void TransformToKvValues(std::vector<T> elements, std::vector<pb::common::KeyValue> &kvs) {
    for (auto &element : elements) {
      pb::common::KeyValue kv;
      kv.set_key(GenKey(element.id()));
      kv.set_value(element.SerializeAsString());
      kvs.push_back(kv);
    }
  }

  void TransformFromKvValue(const pb::common::KeyValue &kv, T &element) { element.ParsePartialFromString(kv.value()); };

  void TransformFromKvValues(const std::vector<pb::common::KeyValue> &kvs, std::vector<T> &elements) {
    for (const auto &kv : kvs) {
      T element;
      element.ParsePartialFromString(kv.value());
      elements.push_back(element);
    }
  };

  butil::Status Get(const int64_t &id, T &element) { return Get(EncodeInt64(id), element); }

  butil::Status Get(const std::string &id, T &element) {
    std::string key = GenKey(id);
    std::string value;

    butil::Status status = raw_engine_->Reader()->KvGet(Constant::kStoreMetaCF, key, value);
    if (!status.ok() && status.error_code() != pb::error::EKEY_NOT_FOUND) {
      DINGO_LOG(ERROR) << fmt::format("Meta get key {} failed, errcode: {} {}", key, status.error_code(),
                                      status.error_str());
      return status;
    }

    if (!element.ParsePartialFromString(value)) {
      DINGO_LOG(ERROR) << fmt::format("Meta parse value failed, key: {}", key);
      return butil::Status(pb::error::EINTERNAL, "Meta parse value failed");
    }

    return butil::Status::OK();
  }

  bool Exists(const int64_t &id) { return Exists(EncodeInt64(id)); }

  bool Exists(const std::string &id) {
    std::string key = GenKey(id);
    std::string value;
    butil::Status status = raw_engine_->Reader()->KvGet(Constant::kStoreMetaCF, key, value);
    if (!status.ok() && status.error_code() != pb::error::EKEY_NOT_FOUND) {
      DINGO_LOG(ERROR) << fmt::format("Meta get key {} failed, errcode: {} {}", key, status.error_code(),
                                      status.error_str());
      return false;
    }

    return !value.empty();
  }

  butil::Status Put(const int64_t &id, const T &element) {
    CHECK(id != 0);
    CHECK(id == element.id());

    pb::common::KeyValue kv;
    TransformToKvValue(element, kv);
    butil::Status status = raw_engine_->Writer()->KvPut(Constant::kStoreMetaCF, kv);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put id {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status Put(const std::string &id, const T &element) {
    CHECK(!id.empty());
    CHECK(id == element.id());

    pb::common::KeyValue kv;
    TransformToKvValue(element, kv);
    butil::Status status = raw_engine_->Writer()->KvPut(Constant::kStoreMetaCF, kv);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put id {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status MultiPut(const std::vector<int64_t> &ids, const std::vector<T> &elements) {
    CHECK(ids.size() == elements.size());

    std::vector<std::string> str_ids;
    str_ids.reserve(ids.size());
    for (const auto &id : ids) {
      str_ids.push_back(EncodeInt64(id));
    }

    return MultiPut(str_ids, elements);
  }

  butil::Status MultiPut(const std::vector<std::string> &ids, const std::vector<T> &elements) {
    CHECK(ids.size() == elements.size());

    std::vector<pb::common::KeyValue> kvs;
    TransformToKvValue(elements, kvs);
    butil::Status status = raw_engine_->Writer()->KvBatchPut(Constant::kStoreMetaCF, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put keys {} failed, errcode: {} {}", ids.size(), status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status PutIfAbsent(const std::string &id, const T &element) {
    pb::common::KeyValue kv;
    TransformToKvValue(element, kv);
    bool key_is_exists;
    butil::Status status = raw_engine_->Writer()->KvPutIfAbsent(Constant::kStoreMetaCF, kv, key_is_exists);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put key {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status PutIfExists(const std::string &id, const T &value) {
    pb::common::KeyValue kv;
    auto is_exists = Exists(id);
    if (!is_exists) {
      return butil::Status(pb::error::EKEY_NOT_FOUND, "key not found");
    }

    TransformToKvValue(value, kv);
    bool key_is_exists;
    butil::Status status = raw_engine_->Writer()->KvPut(Constant::kStoreMetaCF, kv);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put key {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  int64_t Size() { return Count(); }

  int64_t Count() {
    int64_t count = 0;
    std::string start_key = GenKey("");
    std::string end_key = internal_prefix + "~";
    butil::Status status = raw_engine_->Reader()->KvCount(Constant::kStoreMetaCF, start_key, end_key, count);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta get size failed, errcode: {} {}", status.error_code(), status.error_str());
      return 0;
    }

    return count;
  }

  butil::Status Erase(const int64_t &id) { return Erase(EncodeInt64(id)); }

  butil::Status Erase(const std::string &id) {
    std::string key = GenKey(id);
    butil::Status status = raw_engine_->Writer()->KvDelete(Constant::kStoreMetaCF, key);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta delete id {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status Clear() {
    pb::common::Range range;
    range.set_start_key(GenKey(""));
    range.set_end_key(internal_prefix + "~");

    butil::Status status = raw_engine_->Writer()->KvDeleteRange(Constant::kStoreMetaCF, range);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta clear failed, errcode: {} {}", status.error_code(), status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status GetAllIds(std::vector<int64_t> &ids) {
    std::vector<std::string> str_ids;
    auto ret = GetAllIds(str_ids);
    if (!ret.ok()) {
      return ret;
    }

    for (const auto &str_id : str_ids) {
      ids.push_back(DecodeInt64(str_id));
    }

    return butil::Status::OK();
  }

  butil::Status GetAllIds(std::vector<std::string> &ids) {
    std::string start_key = GenKey("");
    std::string end_key = internal_prefix + "~";

    std::vector<pb::common::KeyValue> kvs;
    butil::Status status = raw_engine_->Reader()->KvScan(Constant::kStoreMetaCF, start_key, end_key, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta get all keys failed, errcode: {} {}", status.error_code(),
                                      status.error_str());
      return status;
    }

    for (const auto &kv : kvs) {
      ids.push_back(ParseId(kv.key()));
    }

    return butil::Status::OK();
  }

  butil::Status GetAllIdElements(std::map<int64_t, T> &id_elements) {
    std::map<std::string, T> str_id_elements;
    auto ret = GetAllIdElements(str_id_elements);
    if (!ret.ok()) {
      return ret;
    }

    for (const auto &it : str_id_elements) {
      id_elements.insert_or_assign(DecodeInt64(it.first), it.second);
    }

    return butil::Status::OK();
  }

  butil::Status GetAllIdElements(std::map<std::string, T> &id_elements) {
    std::string start_key = GenKey("");
    std::string end_key = internal_prefix + "~";

    std::vector<pb::common::KeyValue> kvs;
    butil::Status status = raw_engine_->Reader()->KvScan(Constant::kStoreMetaCF, start_key, end_key, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta get all keys failed, errcode: {} {}", status.error_code(),
                                      status.error_str());
      return status;
    }

    for (const auto &kv : kvs) {
      T element;
      if (!element.ParsePartialFromString(kv.value())) {
        DINGO_LOG(ERROR) << fmt::format("Meta parse value failed, key: {}", kv.key());
        return butil::Status(pb::error::EINTERNAL, "Meta parse value failed");
      }

      id_elements.insert_or_assign(ParseId(kv.key()), element);
    }

    return butil::Status::OK();
  }

  butil::Status GetAllElements(std::vector<T> &elements) {
    std::string start_key = GenKey("");
    std::string end_key = internal_prefix + "~";

    std::vector<pb::common::KeyValue> kvs;
    butil::Status status = raw_engine_->Reader()->KvScan(Constant::kStoreMetaCF, start_key, end_key, kvs);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta get all keys failed, errcode: {} {}", status.error_code(),
                                      status.error_str());
      return status;
    }

    for (const auto &kv : kvs) {
      T element;
      if (!element.ParsePartialFromString(kv.value())) {
        DINGO_LOG(ERROR) << fmt::format("Meta parse value failed, key: {}", kv.key());
        return butil::Status(pb::error::EINTERNAL, "Meta parse value failed");
      }

      elements.push_back(element);
    }

    return butil::Status::OK();
  }

  MetaMap(const MetaMap &) = delete;
  const MetaMap &operator=(const MetaMap &) = delete;

 private:
  std::shared_ptr<RawEngine> raw_engine_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_META_STORAGE_H_