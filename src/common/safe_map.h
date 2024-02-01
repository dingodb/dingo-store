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

#ifndef DINGODB_COMMON_SAFE_MAP_H_
#define DINGODB_COMMON_SAFE_MAP_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <utility>
#include <vector>

#include "butil/containers/doubly_buffered_data.h"
#include "butil/containers/flat_map.h"

namespace dingodb {

// Implement a ThreadSafeMap
// Notice: Must call Init(capacity) before use
// all membber functions except Size(), MemorySize() return 1 if success, return -1 if failed
// all inner functions return 1 if success, return 0 if failed
// Size() and MemorySize() return 0 if failed, return size if success
template <typename T_KEY, typename T_VALUE>
class DingoSafeMap {
 public:
  using TypeRawMap = butil::FlatMap<T_KEY, T_VALUE>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeRawMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  DingoSafeMap() = default;
  DingoSafeMap(const DingoSafeMap &) = delete;
  ~DingoSafeMap() { safe_map.Modify(InnerClear); }

  void Init(int64_t capacity) { safe_map.Modify(InnerInit, capacity); }
  void Resize(int64_t capacity) { safe_map.Modify(InnerResize, capacity); }

  // Get
  // get value by key
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
    return 1;
  }

  // multi-get value by key
  int MultiGet(const std::vector<T_KEY> &keys, std::vector<T_VALUE> &values, std::vector<bool> &exists) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (auto key : keys) {
      T_VALUE value;
      auto *value_ptr = ptr->seek(key);
      if (!value_ptr) {
        values.push_back(value);
        exists.push_back(false);
      } else {
        values.push_back(*value_ptr);
        exists.push_back(true);
      }
    }

    return 1;
  }

  // Get
  // get value by key
  T_VALUE Get(const T_KEY &key) {
    TypeScopedPtr ptr;
    T_VALUE value;
    if (safe_map.Read(&ptr) != 0) {
      return value;
    }
    auto *value_ptr = ptr->seek(key);
    if (!value_ptr) {
      return value;
    }

    return *value_ptr;
  }

  // GetAllKeys
  // get all keys of the map
  int GetAllKeys(std::vector<T_KEY> &keys) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      keys.push_back(it->first);
    }

    return keys.size();
  }

  // GetAllKeys
  // get all keys of the map
  int GetAllKeys(std::set<T_KEY> &keys, std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        keys.insert(it->first);
      }
    }

    return keys.size();
  }

  // GetAllValues
  // get all values of the map
  int GetAllValues(std::vector<T_VALUE> &values, std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        values.push_back(it->second);
      }
    }

    return values.size();
  }

  // GetAllKeyValues
  // get all keys and values of the map
  int GetAllKeyValues(std::vector<T_KEY> &keys, std::vector<T_VALUE> &values,
                      std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        keys.push_back(it->first);
        values.push_back(it->second);
      }
    }

    return keys.size();
  }

  int GetAllKeyValues(std::map<T_KEY, T_VALUE> &key_value_map, std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        key_value_map.insert_or_assign(it->first, it->second);
      }
    }

    return key_value_map.size();
  }

  // Exists
  // check if the key exists in the safe map
  bool Exists(const T_KEY &key) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return false;
    }
    auto *value_ptr = ptr->seek(key);
    return static_cast<bool>(value_ptr);
  }

  // SafeExists
  // check if the key exists in the safe map
  int SafeExists(const T_KEY &key, bool &exists) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }
    auto *value_ptr = ptr->seek(key);
    exists = static_cast<bool>(value_ptr);
    return 1;
  }

  // Size
  // return the record count of map
  int64_t Size() {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return 0;
    }

    return ptr->size();
  }

  // MemorySize
  // return the memory size of map
  int64_t MemorySize() {
    TypeScopedPtr ptr;
    int64_t size = 0;
    if (safe_map.Read(&ptr) != 0) {
      return 0;
    }

    for (auto const it : *ptr) {
      size += it.second.ByteSizeLong();
    }
    // safe map is double buffered map, so we need to multiply 2
    return size * 2;
  }

  // Copy
  // copy the map with FlatMap input_map
  int CopyFromRawMap(const TypeRawMap &input_map) {
    if (safe_map.Modify(InnerCopyFromRawMap, input_map) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // GetRawMapCopy
  // get a copy of the internal flat map
  // used to get all key-value pairs from safe map
  // the out_map must be initialized before call this function
  int GetRawMapCopy(TypeRawMap &out_map) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    out_map = *ptr;
    return 1;
  }

  // Put
  // put key-value pair into map
  int Put(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPut, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // MultiPut
  // put key-value pairs into map
  int MultiPut(const std::vector<T_KEY> &key_list, const std::vector<T_VALUE> &value_list) {
    if (safe_map.Modify(InnerMultiPut, key_list, value_list) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // MultiErase
  // erase multi keys
  int MultiErase(const std::vector<T_KEY> &key_list) {
    if (safe_map.Modify(InnerMultiErase, key_list) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfExists
  // put key-value pair into map if key exists
  int PutIfExists(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfExists, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfAbsent
  // put key-value pair into map if key not exists
  int PutIfAbsent(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfAbsent, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfEqual
  // put key-value pair into map if key exists and value equals
  int PutIfEqual(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfEqual, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfNotEqual
  // put key-value pair into map if key exists and value not equals
  int PutIfNotEqual(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfNotEqual, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Erase
  // erase key-value pair from map
  int Erase(const T_KEY &key) {
    if (safe_map.Modify(InnerErase, key) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Erase
  // erase all key-value pairs from map
  int Clear() {
    if (safe_map.Modify(InnerClear) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Overload the [] operator for reading
  // now we can use map[key] to get value
  // but it's hard to implement the [] operator for writing
  T_VALUE operator[](T_KEY &key) const { return Get(key); }

 protected:
  // all inner function return 1 if modify record access, return 0 if no record is successfully modified
  static size_t InnerCopyFromRawMap(TypeRawMap &map, const TypeRawMap &input_map) {
    map = input_map;
    return 1;
  }

  static size_t InnerErase(TypeRawMap &map, const T_KEY &key) {
    map.erase(key);
    return 1;
  }

  static size_t InnerClear(TypeRawMap &map) {
    map.clear();
    return 1;
  }

  static size_t InnerPut(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    map.insert(key, value);
    return 1;
  }

  static size_t InnerMultiPut(TypeRawMap &map, const std::vector<T_KEY> &key_list,
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

  static size_t InnerMultiErase(TypeRawMap &map, const std::vector<T_KEY> &key_list) {
    if (key_list.empty()) {
      return 0;
    }

    for (int i = 0; i < key_list.size(); i++) {
      map.erase(key_list[i]);
    }
    return 1;
  }

  static size_t InnerPutIfExists(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      return 0;
    }

    *value_ptr = value;
    return 1;
  }

  static size_t InnerPutIfAbsent(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr != nullptr) {
      return 0;
    }

    map.insert(key, value);
    return 1;
  }

  static size_t InnerPutIfEqual(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      return 0;
    }

    if (*value_ptr != value) {
      return 0;
    }

    return 1;
  }

  static size_t InnerPutIfNotEqual(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      return 0;
    }

    if (*value_ptr == value) {
      return 0;
    }

    *value_ptr = value;
    return 1;
  }

  static size_t InnerInit(TypeRawMap &m, const int64_t &capacity) {
    CHECK_EQ(0, m.init(capacity));
    return 1;
  }

  static size_t InnerResize(TypeRawMap &m, const int64_t &capacity) {
    CHECK_EQ(0, m.resize(capacity));
    return 1;
  }

  // This is the double bufferd map, it's lock-free
  // But must modify data using Modify function
  TypeSafeMap safe_map;
};

// Implement a ThreadSafeMap
// Notice: Must call Init(capacity) before use
// all membber functions except Size(), MemorySize() return 1 if success, return -1 if failed
// all inner functions return 1 if success, return 0 if failed
// Size() and MemorySize() return 0 if failed, return size if success
template <typename T_KEY, typename T_VALUE>
class DingoSafeStdMap {
 public:
  using TypeRawMap = std::map<T_KEY, T_VALUE>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeRawMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  DingoSafeStdMap() = default;
  DingoSafeStdMap(const DingoSafeStdMap &) = delete;
  ~DingoSafeStdMap() { safe_map.Modify(InnerClear); }

  // void Init(int64_t capacity) { safe_map.Modify(InnerInit, capacity); }
  // void Resize(int64_t capacity) { safe_map.Modify(InnerResize, capacity); }

  // Get
  // get value by key
  int Get(const T_KEY &key, T_VALUE &value) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }
    auto value_iter = ptr->find(key);
    if (value_iter == ptr->end()) {
      return -1;
    }

    value = value_iter->second;
    return 1;
  }

  // multi-get value by key
  int MultiGet(const std::vector<T_KEY> &keys, std::vector<T_VALUE> &values, std::vector<bool> &exists) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (auto key : keys) {
      T_VALUE value;
      auto value_iter = ptr->find(key);
      if (value_iter == ptr->end()) {
        values.push_back(value);
        exists.push_back(false);
      } else {
        values.push_back(value_iter->second);
        exists.push_back(true);
      }
    }

    return 1;
  }

  // Get
  // get value by key
  T_VALUE Get(const T_KEY &key) {
    TypeScopedPtr ptr;
    T_VALUE value;
    if (safe_map.Read(&ptr) != 0) {
      return value;
    }
    auto value_iter = ptr->find(key);
    if (value_iter == ptr->end()) {
      return value;
    }

    return value_iter->second;
  }

  // GetAllKeys
  // get all keys of the map
  int GetAllKeys(std::vector<T_KEY> &keys) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      keys.push_back(it->first);
    }

    return keys.size();
  }

  // GetFirstKey
  // get the first key of the internal std::map
  int GetFirstKey(T_KEY &key) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    if (ptr->empty()) {
      return 0;
    }

    key = ptr->begin()->first;
    return 1;
  }

  // GetLastKey
  // get last key of the internal std::map
  int GetLastKey(T_KEY &key) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    if (ptr->empty()) {
      return 0;
    }

    key = ptr->rbegin()->first;
    return 1;
  }

  // GetAllKeys
  // get all keys of the map
  int GetAllKeys(std::set<T_KEY> &keys, std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        keys.insert(it->first);
      }
    }

    return keys.size();
  }

  // GetAllValues
  // get all values of the map
  int GetAllValues(std::vector<T_VALUE> &values, std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        values.push_back(it->second);
      }
    }

    return values.size();
  }

  // GetAllKeyValues
  // get all keys and values of the map
  int GetAllKeyValues(std::vector<T_KEY> &keys, std::vector<T_VALUE> &values,
                      std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        keys.push_back(it->first);
        values.push_back(it->second);
      }
    }

    return keys.size();
  }

  int GetAllKeyValues(std::map<T_KEY, T_VALUE> &key_values, std::function<bool(T_VALUE)> filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeRawMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (filter == nullptr || filter(it->second)) {
        key_values.insert_or_assign(it->first, it->second);
      }
    }

    return key_values.size();
  }

  // GetRangeKeys
  // get keys of range
  int GetRangeKeys(std::set<T_KEY> &keys, T_KEY lower_bound, T_KEY upper_bound,
                   std::function<bool(T_KEY)> key_filter = nullptr,
                   std::function<bool(T_VALUE)> value_filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    typename TypeRawMap::iterator it = ptr->lower_bound(lower_bound);
    for (; it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (it->first >= upper_bound) {
        break;
      }
      if ((key_filter == nullptr || key_filter(it->first)) & (value_filter == nullptr || value_filter(it->second))) {
        keys.insert(it->first);
      }
    }

    return keys.size();
  }

  // GetRangeValues
  // get values of range
  int GetRangeValues(std::vector<T_VALUE> &values, T_KEY lower_bound, T_KEY upper_bound,
                     std::function<bool(T_KEY)> key_filter = nullptr,
                     std::function<bool(T_VALUE)> value_filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    typename TypeRawMap::const_iterator it = ptr->lower_bound(lower_bound);
    for (; it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (it->first >= upper_bound) {
        break;
      }
      if ((key_filter == nullptr || key_filter(it->first)) & (value_filter == nullptr || value_filter(it->second))) {
        values.push_back(it->second);
      }
    }

    return values.size();
  }

  // FinIntervalValues
  // The real range is [lower_bound, upper_bound)
  int FindIntervalValues(std::vector<T_VALUE> &values, T_KEY lower_bound, T_KEY upper_bound,
                         std::function<bool(T_KEY)> key_filter = nullptr,
                         std::function<bool(T_VALUE)> value_filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    if (ptr->empty() || lower_bound >= upper_bound) {
      return 0;
    }

    typename TypeRawMap::const_iterator it = ptr->lower_bound(lower_bound);
    if (it == ptr->end()) {
      auto reverse_it = ptr->rbegin();
      if (reverse_it == ptr->rend()) {
        return 0;
      }
      if (reverse_it->first >= upper_bound) {
        return 0;
      } else {
        if ((key_filter == nullptr || key_filter(reverse_it->first)) &
            (value_filter == nullptr || value_filter(reverse_it->second))) {
          values.push_back(reverse_it->second);
        }
        return values.size();
      }
    } else {
      if (it->first > lower_bound) {
        if (it != ptr->begin()) {
          --it;
        }
      }
      for (; it != ptr->end(); ++it) {
        if (it == ptr->end()) {
          break;
        }
        if (it->first >= upper_bound) {
          break;
        }
        if ((key_filter == nullptr || key_filter(it->first)) & (value_filter == nullptr || value_filter(it->second))) {
          values.push_back(it->second);
        }
      }

      return values.size();
    }

    return values.size();
  }

  // GetRangeKeyValues
  // get keys and values of range
  int GetRangeKeyValues(std::vector<T_KEY> &keys, std::vector<T_VALUE> &values, T_KEY lower_bound, T_KEY upper_bound,
                        std::function<bool(T_KEY)> key_filter = nullptr,
                        std::function<bool(T_VALUE)> value_filter = nullptr) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    typename TypeRawMap::const_iterator it = ptr->lower_bound(lower_bound);
    for (; it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      if (it->first >= upper_bound) {
        break;
      }
      if ((key_filter == nullptr || key_filter(it->first)) & (value_filter == nullptr || value_filter(it->second))) {
        keys.push_back(it->first);
        values.push_back(it->second);
      }
    }

    return keys.size();
  }

  // Exists
  // check if the key exists in the safe map
  bool Exists(const T_KEY &key) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return false;
    }
    auto it = ptr->find(key);
    return static_cast<bool>(it != ptr->end());
  }

  // Size
  // return the record count of map
  int64_t Size() {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return 0;
    }

    return ptr->size();
  }

  // MemorySize
  // return the memory size of map
  int64_t MemorySize() {
    TypeScopedPtr ptr;
    int64_t size = 0;
    if (safe_map.Read(&ptr) != 0) {
      return 0;
    }

    for (auto const it : *ptr) {
      size += it.second.ByteSizeLong();
    }
    // safe map is double buffered map, so we need to multiply 2
    return size * 2;
  }

  // Copy
  // copy the map with FlatMap input_map
  int CopyFromRawMap(const TypeRawMap &input_map) {
    if (safe_map.Modify(InnerCopyFromRawMap, input_map) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // GetRawMapCopy
  // get a copy of the internal flat map
  // used to get all key-value pairs from safe map
  // the out_map must be initialized before call this function
  int GetRawMapCopy(TypeRawMap &out_map) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    out_map = *ptr;
    return 1;
  }

  // Put
  // put key-value pair into map
  int Put(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPut, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // MultiPut
  // put key-value pairs into map
  int MultiPut(const std::vector<T_KEY> &key_list, const std::vector<T_VALUE> &value_list) {
    if (safe_map.Modify(InnerMultiPut, key_list, value_list) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // MultiErase
  // erase multi keys
  int MultiErase(const std::vector<T_KEY> &key_list) {
    if (safe_map.Modify(InnerMultiErase, key_list) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // MultiEraseThenPut
  // erase multi keys
  int MultiEraseThenPut(const std::vector<T_KEY> &key_list_delete, const std::vector<T_KEY> &key_list_put,
                        const std::vector<T_VALUE> &value_list_put) {
    if (key_list_put.size() != value_list_put.size()) {
      return -1;
    }
    std::map<T_KEY, T_VALUE> map;
    for (int i = 0; i < key_list_put.size(); i++) {
      map.insert(std::make_pair(key_list_put[i], value_list_put[i]));
    }
    if (safe_map.Modify(InnerMultiEraseThenPut, key_list_delete, map) > 0) {
      return 1;
    }

    return -1;
  }

  // PutIfExists
  // put key-value pair into map if key exists
  int PutIfExists(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfExists, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfAbsent
  // put key-value pair into map if key not exists
  int PutIfAbsent(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfAbsent, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfEqual
  // put key-value pair into map if key exists and value equals
  int PutIfEqual(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfEqual, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // PutIfNotEqual
  // put key-value pair into map if key exists and value not equals
  int PutIfNotEqual(const T_KEY &key, const T_VALUE &value) {
    if (safe_map.Modify(InnerPutIfNotEqual, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Erase
  // erase key-value pair from map
  int Erase(const T_KEY &key) {
    if (safe_map.Modify(InnerErase, key) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Erase
  // erase all key-value pairs from map
  int Clear() {
    if (safe_map.Modify(InnerClear) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Overload the [] operator for reading
  // now we can use map[key] to get value
  // but it's hard to implement the [] operator for writing
  T_VALUE operator[](T_KEY &key) const { return Get(key); }

 protected:
  // all inner function return 1 if modify record access, return 0 if no record is successfully modified
  static size_t InnerCopyFromRawMap(TypeRawMap &map, const TypeRawMap &input_map) {
    map = input_map;
    return 1;
  }

  static size_t InnerErase(TypeRawMap &map, const T_KEY &key) {
    map.erase(key);
    return 1;
  }

  static size_t InnerClear(TypeRawMap &map) {
    map.clear();
    return 1;
  }

  static size_t InnerPut(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    map.insert_or_assign(key, value);
    return 1;
  }

  static size_t InnerMultiPut(TypeRawMap &map, const std::vector<T_KEY> &key_list,
                              const std::vector<T_VALUE> &value_list) {
    if (key_list.size() != value_list.size()) {
      return 0;
    }

    if (key_list.empty()) {
      return 0;
    }

    for (int i = 0; i < key_list.size(); i++) {
      map.insert_or_assign(key_list[i], value_list[i]);
    }
    return key_list.size();
  }

  static size_t InnerMultiEraseThenPut(TypeRawMap &map, const std::vector<T_KEY> &key_list_delete,
                                       const std::map<T_KEY, T_VALUE> &kv_put_map) {
    if (kv_put_map.empty() && key_list_delete.empty()) {
      return 1;
    }

    for (int i = 0; i < key_list_delete.size(); i++) {
      map.erase(key_list_delete[i]);
    }

    for (const auto &kv : kv_put_map) {
      map.insert_or_assign(kv.first, kv.second);
    }

    return 1;
  }

  static size_t InnerMultiErase(TypeRawMap &map, const std::vector<T_KEY> &key_list) {
    if (key_list.empty()) {
      return 0;
    }

    for (int i = 0; i < key_list.size(); i++) {
      map.erase(key_list[i]);
    }
    return 1;
  }

  static size_t InnerPutIfExists(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto iter = map.find(key);
    if (iter == map.end()) {
      return 0;
    }

    iter->second = value;
    return 1;
  }

  static size_t InnerPutIfAbsent(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto iter = map.find(key);
    if (iter != map.end()) {
      return 0;
    }

    map.insert_or_assign(key, value);
    return 1;
  }

  static size_t InnerPutIfEqual(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto iter = map.find(key);
    if (iter == map.end()) {
      return 0;
    }

    if (iter != value) {
      return 0;
    }

    return 1;
  }

  static size_t InnerPutIfNotEqual(TypeRawMap &map, const T_KEY &key, const T_VALUE &value) {
    auto iter = map.find(key);
    if (iter == map.end()) {
      return 0;
    }

    if (iter->second == value) {
      return 0;
    }

    iter->second = value;
    return 1;
  }

  // static size_t InnerInit(TypeRawMap &m, const int64_t &capacity) {
  //   CHECK_EQ(0, m.init(capacity));
  //   return 1;
  // }

  // static size_t InnerResize(TypeRawMap &m, const int64_t &capacity) {
  //   CHECK_EQ(0, m.resize(capacity));
  //   return 1;
  // }

  // This is the double bufferd map, it's lock-free
  // But must modify data using Modify function
  TypeSafeMap safe_map;
};

}  // namespace dingodb

#endif  // DINGODB_COMMON_SAFE_MAP_H_