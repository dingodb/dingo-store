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
#include "common/logging.h"
#include "common/safe_map.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"

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

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_META_STORAGE_H_