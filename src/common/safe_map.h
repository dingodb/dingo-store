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

}  // namespace dingodb

#endif  // DINGODB_COMMON_SAFE_MAP_H_