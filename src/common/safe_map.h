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

#include <cstdint>
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
  using TypeFlatMap = butil::FlatMap<T_KEY, T_VALUE>;
  using TypeSafeMap = butil::DoublyBufferedData<TypeFlatMap>;
  using TypeScopedPtr = typename TypeSafeMap::ScopedPtr;

  DingoSafeMap() = default;
  DingoSafeMap(const DingoSafeMap &) = delete;
  ~DingoSafeMap() { safe_map.Modify(InnerClear); }

  void Init(uint64_t capacity) { safe_map.Modify(InnerInit, capacity); }
  void Resize(uint64_t capacity) { safe_map.Modify(InnerResize, capacity); }

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

    for (typename TypeFlatMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      keys->push_back(it->first);
    }

    return keys.size();
  }

  // GetAllKeyValues
  // get all keys and values of the map
  int GetAllKeyValues(std::vector<T_KEY> &keys, std::vector<T_VALUE> &values) {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return -1;
    }

    for (typename TypeFlatMap::const_iterator it = ptr->begin(); it != ptr->end(); ++it) {
      if (it == ptr->end()) {
        break;
      }
      keys->push_back(it->first);
      values->push_back(it->second);
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
    auto *value_ptr = ptr->seek(key);
    return static_cast<bool>(value_ptr);
  }

  // Size
  // return the record count of map
  uint64_t Size() {
    TypeScopedPtr ptr;
    if (safe_map.Read(&ptr) != 0) {
      return 0;
    }

    return ptr->size();
  }

  // MemorySize
  // return the memory size of map
  uint64_t MemorySize() {
    TypeScopedPtr ptr;
    uint64_t size = 0;
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
  int CopyFlatMap(const TypeFlatMap &input_map) {
    if (safe_map.Modify(InnerCopyFlatMap, input_map) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Copy
  // copy the map with SafeMap input_map
  int Copy(const TypeSafeMap &input_map) {
    if (safe_map.Modify(InnerCopySafeMap, input_map) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // GetFlatMapCopy
  // get a copy of the internal flat map
  // used to get all key-value pairs from safe map
  // the out_map must be initialized before call this function
  int GetFlatMapCopy(TypeFlatMap &out_map) {
    // if (safe_map.Modify(InnerGetFlatMapCopy, out_map) > 0) {
    //   return 1;
    // } else {
    //   return -1;
    // }

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
  static size_t InnerCopyFlatMap(TypeFlatMap &map, const TypeFlatMap &input_map) {
    map = input_map;
    return 1;
  }

  // static size_t InnerGetFlatMapCopy(TypeFlatMap &map, const TypeFlatMap &out_map) {
  //   // Notice: The brpc's template restrict to return value in Modify process, but we need to do this, so use a
  //   // const_cast to modify the input parameter here
  //   auto &mutable_out_map = const_cast<TypeFlatMap &>(out_map);

  //   if (!static_cast<bool>(mutable_out_map.initialized())) {
  //     mutable_out_map.init(1024);
  //   }
  //   mutable_out_map = map;
  //   return 1;
  // }

  static size_t InnerCopySafeMap(TypeFlatMap &map, const TypeSafeMap &input_map) {
    // Notice: The brpc's template restrict to return value in Modify process, but we need to do this, so use a
    // const_cast to modify the input parameter here
    auto &mutable_input_map = const_cast<TypeSafeMap &>(input_map);
    mutable_input_map.CopyFlatMap(map);
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

    map.insert(key, value);
    return 1;
  }

  static size_t InnerPutIfEqual(TypeFlatMap &map, const T_KEY &key, const T_VALUE &value) {
    auto *value_ptr = map.seek(key);
    if (value_ptr == nullptr) {
      return 0;
    }

    if (*value_ptr != value) {
      return 0;
    }

    *value_ptr = value;
    return 1;
  }

  static size_t InnerPutIfNotEqual(TypeFlatMap &map, const T_KEY &key, const T_VALUE &value) {
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

}  // namespace dingodb

#endif  // DINGODB_COMMON_SAFE_MAP_H_