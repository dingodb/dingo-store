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
#include <vector>

#include "butil/containers/doubly_buffered_data.h"
#include "butil/containers/flat_map.h"
#include "butil/strings/stringprintf.h"
#include "common/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator_internal.pb.h"

namespace dingodb {

// Implement a ThreadSafeMap
// Notice: Must call Init(capacity) before use
template <typename T_KEY, typename T_VALUE>
class DingoSafeMap {
 public:
  using TypeFlatMap = butil::FlatMap<T_KEY, T_VALUE>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeFlatMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  DingoSafeMap() = default;
  DingoSafeMap(const DingoSafeMap &) = delete;
  ~DingoSafeMap() { safe_map.Modify(InnerClear); }

  void Init(uint64_t capacity) { safe_map.Modify(InnerInit, capacity); }
  void Resize(uint64_t capacity) { safe_map.Modify(InnerResize, capacity); }

  int Get(const T_KEY &key, T_VALUE &value) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }
    auto *value_ptr = ptr->seek(key);
    if (!value_ptr) {
      return -1;
    }

    value = *value_ptr;
    return 0;
  }

  int Size(uint64_t &size) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    size = ptr->size();
    return 1;
  }

  int MemorySize(uint64_t &size) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (auto const it : *ptr) {
      size += it.second.ByteSizeLong();
    }
    return 1;
  }

  int Swap(const TypeFlatMap &input_map) { return safe_map.Modify(InnerSwap, input_map); }

  int Copy(const TypeFlatMap &input_map) { return safe_map.Modify(InnerCopy, input_map); }

  int Put(const T_KEY &key, const T_VALUE &value) { return safe_map.Modify(InnerPut, key, value); }

  int MultiPut(const std::vector<T_KEY> &key_list, const std::vector<T_VALUE> &value_list) {
    return safe_map.Modify(InnerMultiPut, key_list, value_list);
  }

  int PutIfExists(const T_KEY &key, const T_VALUE &value) { return safe_map.Modify(InnerPutIfExists, key, value); }

  int PutIfAbsent(const T_KEY &key, const T_VALUE &value) { return safe_map.Modify(InnerPutIfAbsent, key, value); }

  int Erase(const T_KEY &key, const T_VALUE &value) { return safe_map.Modify(InnerErase, key, value); }

 protected:
  static size_t InnerSwap(TypeFlatMap &map, const TypeFlatMap &input_map) {
    map.swap(input_map);
    return 1;
  }

  static size_t InnerCopy(TypeFlatMap &map, const TypeFlatMap &input_map) {
    map = input_map;
    return 1;
  }

  static size_t InnerErase(TypeFlatMap &map, const T_KEY &key) {
    map.erase(key);
    return 1;
  }

  static size_t InnerClear(TypeFlatMap &map) {
    map.clear();
    return 1;
  }

  static size_t InnerPut(TypeFlatMap &map, const T_KEY &key, const T_VALUE &value) {
    map.insert(key, value);
    return 1;
  }

  static size_t InnerMultiPut(TypeFlatMap &map, const std::vector<T_KEY> &key_list,
                              const std::vector<T_VALUE> &value_list) {
    if (key_list.size() != value_list.size()) {
      return 0;
    }

    if (key_list.empty()) {
      return 0;
    }

    for (int i = 0; i < key_list.size(); i++) {
      map.insert(key_list[i], value_list[i]);
    }
    return key_list.size();
  }

  static size_t InnerPutIfExists(TypeFlatMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      return 0;
    }

    *value_ptr = value;
    return 1;
  }

  static size_t InnerPutIfAbsent(TypeFlatMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr != nullptr) {
      return 0;
    }

    *value_ptr = value;
    return 1;
  }

  static size_t InnerInit(TypeFlatMap &m, const uint64_t &capacity) {
    CHECK_EQ(0, m.init(capacity));
    return 1;
  }

  static size_t InnerResize(TypeFlatMap &m, const uint64_t &capacity) {
    CHECK_EQ(0, m.resize(capacity));
    return 1;
  }

  // This is the double bufferd map, it's lock-free
  // But must modify data using Modify function
  TypeSafeMap safe_map;
};

#define COORDINATOR_ID_OF_MAP_MIN 1000

// Implement a ThreadSafeMap
// This is for IdEpoch with atomic GetNextId function
class DingoSafeIdEpochMap : public DingoSafeMap<uint64_t, pb::coordinator_internal::IdEpochInternal> {
 public:
  using TypeFlatMap = butil::FlatMap<uint64_t, pb::coordinator_internal::IdEpochInternal>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeFlatMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  int GetPresentId(const uint64_t &key, uint64_t &value) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    auto *value_ptr = ptr->seek(key);
    if (!value_ptr) {
      // if not exist, return 0
      value = 0;
      return 0;
    } else {
      value = value_ptr->value();
    }

    return 0;
  }

  int GetNextId(const uint64_t &key, uint64_t &value) { return safe_map.Modify(InnerGetNextId, key, value); }

 private:
  static size_t InnerGetNextId(TypeFlatMap &map, const uint64_t &key, const uint64_t &value) {
    // Notice: The brpc's template restrict to return value in Modify process, but we need to do this, so use a
    // const_cast to modify the input parameter here
    auto &value_mod = const_cast<uint64_t &>(value);

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
};

// MetaMapStorage is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaSafeMapStorage {
 public:
  const std::string internal_prefix;
  MetaSafeMapStorage(butil::FlatMap<uint64_t, T> *elements_ptr)
      : internal_prefix(typeid(T).name()), elements_(elements_ptr){};
  MetaSafeMapStorage(butil::FlatMap<uint64_t, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(prefix), elements_(elements_ptr){};
  ~MetaSafeMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
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

  std::string GenKey(uint64_t id) { return butil::StringPrintf("%s_%lu", internal_prefix.c_str(), id); }

  std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t id) {
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
    std::vector<uint64_t> key_list;
    std::vector<T> value_list;

    for (const auto &kv : kvs) {
      uint64_t id = ParseId(kv.key());
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
  // std::map<uint64_t, std::shared_ptr<T>> *elements_;
  DingoSafeMap<uint64_t, T> *elements_;
};

// MetaMapStorage is a meta storage based on map.
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaMapStorage {
 public:
  const std::string internal_prefix;
  MetaMapStorage(butil::FlatMap<uint64_t, T> *elements_ptr)
      : internal_prefix(typeid(T).name()), elements_(elements_ptr){};
  MetaMapStorage(butil::FlatMap<uint64_t, T> *elements_ptr, const std::string &prefix)
      : internal_prefix(prefix), elements_(elements_ptr){};
  ~MetaMapStorage() = default;

  std::string Prefix() { return internal_prefix; }

  bool Init() {
    DINGO_LOG(INFO) << "coordinator server meta";
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

  std::string GenKey(uint64_t id) { return butil::StringPrintf("%s_%lu", internal_prefix.c_str(), id); }

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
      // elements_->insert_or_assign(id, element);
      elements_->insert(id, element);
    }
  };

  MetaMapStorage(const MetaMapStorage &) = delete;
  const MetaMapStorage &operator=(const MetaMapStorage &) = delete;

 private:
  // Coordinator all region meta data in this server.
  // std::map<uint64_t, std::shared_ptr<T>> *elements_;
  butil::FlatMap<uint64_t, T> *elements_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_META_STORAGE_H_