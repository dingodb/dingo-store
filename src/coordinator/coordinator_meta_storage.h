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

#include "butil/compiler_specific.h"
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
#include "proto/error.pb.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {

#define COORDINATOR_ID_OF_MAP_MIN 1000
#define COORDINATOR_GET_NEXT_IDS_MAX_BATCH 100000

inline std::string EncodeInt64Id(int64_t id) {
  Buf buf(sizeof(int64_t));
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, id);
  return buf.GetString();
}

inline int64_t DecodeInt64Id(const std::string &str_id) {
  CHECK(str_id.size() == sizeof(int64_t));

  Buf buf(str_id);

  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

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
    if (BAIDU_UNLIKELY(key <= 0)) {
      return -1;
    }

    if (safe_map.Modify(InnerGetNextId, key, value) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  int GetNextIds(const int64_t &key, const int64_t &count, std::vector<int64_t> &values) {
    if (BAIDU_UNLIKELY(count <= 0 || key <= 0 || count > COORDINATOR_GET_NEXT_IDS_MAX_BATCH)) {
      return -1;
    }

    std::pair<int64_t, int64_t> key_and_count{key, count};
    if (safe_map.Modify(InnerGetNextIds, key_and_count, values) > 0) {
      return 1;
    } else {
      return -1;
    }
  }

  // Update the present id if input value is larger than the value in map
  // if the update value is equal the internal value, return 0;
  // if the update value is larger than the internal value, return 1;
  // if the update value is less than the internal value, return -1;
  int UpdatePresentId(const int64_t &key, const int64_t &value) {
    if (BAIDU_UNLIKELY(key <= 0)) {
      return -1;
    }

    int ret = safe_map.Modify(InnerUpdatePresentId, key, value);
    return ret;
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

// MetaDiskMap is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaDiskMap {
 public:
  const std::string internal_prefix;
  MetaDiskMap(const std::string &prefix, std::shared_ptr<RawEngine> raw_engine)
      : internal_prefix(std::string("METADSK") + prefix), raw_engine_(raw_engine){};
  ~MetaDiskMap() = default;

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

    return DecodeInt64Id(str_id);
  }

  std::string GenKey(std::string id) { return internal_prefix + "_" + id; }
  std::string GenKey(int64_t id) { return internal_prefix + "_" + EncodeInt64Id(id); }

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

  butil::Status Get(const int64_t &id, T &element) { return Get(EncodeInt64Id(id), element); }

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

  bool Exists(const int64_t &id) { return Exists(EncodeInt64Id(id)); }

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
      str_ids.push_back(EncodeInt64Id(id));
    }

    return MultiPut(str_ids, elements);
  }

  butil::Status MultiPut(const std::vector<std::string> &ids, const std::vector<T> &elements) {
    CHECK(ids.size() == elements.size());

    std::vector<pb::common::KeyValue> kvs;
    TransformToKvValue(elements, kvs);
    butil::Status status = raw_engine_->Writer()->KvBatchPutAndDelete(Constant::kStoreMetaCF, kvs, {});
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

    std::string tmp_value;

    auto ret1 = raw_engine_->Reader()->KvGet(Constant::kStoreMetaCF, kv.key(), *kv.mutable_value());
    if (ret1.ok()) {
      return butil::Status::OK();
    } else if (ret1.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
      // do put
      butil::Status status = raw_engine_->Writer()->KvPut(Constant::kStoreMetaCF, kv);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("Meta put key {} failed, errcode: {} {}", id, status.error_code(),
                                        status.error_str());
        return status;
      }

      return butil::Status::OK();
    } else {
      return ret1;
    }

    return butil::Status::OK();
  }

  butil::Status PutIfExists(const std::string &id, const T &value) {
    pb::common::KeyValue kv;
    auto is_exists = Exists(id);
    if (!is_exists) {
      return butil::Status::OK();
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

  butil::Status Erase(const int64_t &id) { return Erase(EncodeInt64Id(id)); }

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
      ids.push_back(DecodeInt64Id(str_id));
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
      id_elements.insert_or_assign(DecodeInt64Id(it.first), it.second);
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

  MetaDiskMap(const MetaDiskMap &) = delete;
  const MetaDiskMap &operator=(const MetaDiskMap &) = delete;

 private:
  std::shared_ptr<RawEngine> raw_engine_;
};

// MetaMemMapFlat is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaMemMapFlat {
 public:
  const std::string internal_prefix;
  MetaMemMapFlat(DingoSafeMap<int64_t, T> *elements, const std::string &prefix, std::shared_ptr<RawEngine> raw_engine)
      : internal_prefix(std::string("METAFLT") + prefix), raw_engine_(raw_engine), elements_(elements){};
  ~MetaMemMapFlat() = default;

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    elements_->Clear();

    std::vector<int64_t> key_list;
    std::vector<T> value_list;

    for (const auto &kv : kvs) {
      int64_t id = ParseIntId(kv.key());
      T element;
      element.ParsePartialFromString(kv.value());
      key_list.push_back(id);
      value_list.push_back(element);
    }

    elements_->MultiPut(key_list, value_list);

    return true;
  }

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

  butil::Status Get(const int64_t &id, T &element) {
    elements_->Get(id, element);
    return butil::Status::OK();
  }

  bool Exists(const int64_t &id) { return elements_->Exists(id); }

  butil::Status Put(const int64_t &id, const T &element) {
    CHECK(id != 0);
    CHECK(id == element.id());

    auto ret = elements_->Put(id, element);
    if (ret < 0) {
      return butil::Status(pb::error::EINTERNAL, "Meta put failed");
    }

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

    auto ret = elements_->MultiPut(ids, elements);
    if (ret < 0) {
      return butil::Status(pb::error::EINTERNAL, "Meta multi put failed");
    }

    std::vector<std::string> str_ids;
    str_ids.reserve(ids.size());
    for (const auto &id : ids) {
      str_ids.push_back(EncodeInt64(id));
    }

    return MultiPut(str_ids, elements);
  }

  butil::Status PutIfAbsent(const std::string &id, const T &element) {
    CHECK(!id.empty());
    CHECK(id == element.id());

    auto ret = elements_->PutIfAbsent(id, element);
    if (ret < 0) {
      return butil::Status::OK();
    }

    pb::common::KeyValue kv;
    TransformToKvValue(element, kv);

    butil::Status status = raw_engine_->Writer()->KvPut(Constant::kStoreMetaCF, kv);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put key {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status PutIfExists(const int64_t &id, const T &value) {
    pb::common::KeyValue kv;
    auto is_exists = Exists(id);
    if (!is_exists) {
      return butil::Status::OK();
    }

    elements_->PutIfExists(id, value);

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

  int64_t Count() { return elements_->Count(); }

  butil::Status Erase(const int64_t &id) {
    elements_->Erase(id);

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
    elements_->Clear();

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
    elements_->GetAllKeys(ids);
    return butil::Status::OK();
  }

  butil::Status GetAllIdElements(std::map<int64_t, T> &id_elements) {
    elements_->GetAllKeyValues(id_elements);
    return butil::Status::OK();
  }

  butil::Status GetAllElements(std::vector<T> &elements) {
    elements_->GetAllValues(elements);
    return butil::Status::OK();
  }

  MetaMemMapFlat(const MetaMemMapFlat &) = delete;
  const MetaMemMapFlat &operator=(const MetaMemMapFlat &) = delete;

 private:
  std::shared_ptr<RawEngine> raw_engine_;
  DingoSafeMap<int64_t, T> *elements_;
};

// MetaMemMapStd is a template class for meta storage
// This is for read/write meta data from/to RocksDB storage
template <typename T>
class MetaMemMapStd {
 public:
  const std::string internal_prefix;
  MetaMemMapStd(DingoSafeStdMap<std::string, T> *elements, const std::string &prefix,
                std::shared_ptr<RawEngine> raw_engine)
      : internal_prefix(std::string("METASFM") + prefix), raw_engine_(raw_engine), elements_(elements){};
  ~MetaMemMapStd() = default;

  bool Recover(const std::vector<pb::common::KeyValue> &kvs) {
    elements_->Clear();

    std::vector<std::string> key_list;
    std::vector<T> value_list;

    for (const auto &kv : kvs) {
      std::string id = ParseId(kv.key());
      T element;
      element.ParsePartialFromString(kv.value());
      key_list.push_back(id);
      value_list.push_back(element);
    }

    elements_->MultiPut(key_list, value_list);

    return true;
  }

  std::string Prefix() { return internal_prefix; }

  std::string ParseId(const std::string &key) {
    if (key.size() <= internal_prefix.size()) {
      DINGO_LOG(ERROR) << "Parse id failed, invalid str " << key;
      return std::string();
    }

    return key.substr(internal_prefix.size() + 1);
  }

  std::string GenKey(std::string id) { return internal_prefix + "_" + id; }

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

  butil::Status Get(const std::string &id, T &element) {
    elements_->Get(id, element);
    return butil::Status::OK();
  }

  bool Exists(const std::string &id) { return elements_->find(id) != elements_->end(); }

  butil::Status Put(const std::string &id, const T &element) {
    CHECK(!id.empty());
    CHECK(id == element.id());

    auto ret = elements_->Put(id, element);
    if (ret < 0) {
      return butil::Status(pb::error::EINTERNAL, "Meta put failed");
    }

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

  butil::Status MultiPut(const std::vector<std::string> &ids, const std::vector<T> &elements) {
    CHECK(ids.size() == elements.size());

    auto ret = elements_->MultiPut(ids, elements);
    if (ret < 0) {
      return butil::Status(pb::error::EINTERNAL, "Meta multi put failed");
    }

    std::vector<pb::common::KeyValue> kvs;
    TransformToKvValue(elements, kvs);
    butil::Status status = raw_engine_->Writer()->KvBatchPutAndDelete(Constant::kStoreMetaCF, kvs, {});
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put keys {} failed, errcode: {} {}", ids.size(), status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status PutIfAbsent(const std::string &id, const T &element) {
    auto ret = elements_->PutIfAbsent(id, element);
    if (ret < 0) {
      return butil::Status::OK();
    }

    pb::common::KeyValue kv;
    TransformToKvValue(element, kv);
    butil::Status status = raw_engine_->Writer()->KvPut(Constant::kStoreMetaCF, kv);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Meta put key {} failed, errcode: {} {}", id, status.error_code(),
                                      status.error_str());
      return status;
    }

    return butil::Status::OK();
  }

  butil::Status PutIfExists(const std::string &id, const T &value) {
    auto is_exists = Exists(id);
    if (!is_exists) {
      return butil::Status::OK();
    }

    auto ret = elements_->PutIfExists(id, value);
    if (ret < 0) {
      return butil::Status(pb::error::EINTERNAL, "Meta put failed");
    }

    pb::common::KeyValue kv;
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

  int64_t Count() { return elements_->Size(); }

  butil::Status Erase(const std::string &id) {
    elements_->Erase(id);

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
    elements_->Clear();

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

  butil::Status GetAllIds(std::vector<std::string> &ids) {
    elements_->GetAllKeys(ids);
    return butil::Status::OK();
  }

  butil::Status GetAllIdElements(std::map<std::string, T> &id_elements) {
    elements_->GetAllKeyValues(id_elements);
    return butil::Status::OK();
  }

  butil::Status GetAllElements(std::vector<T> &elements) {
    elements_->GetAllValues(elements);
    return butil::Status::OK();
  }

  butil::Status GetRangeValues(std::vector<T> &values, std::string lower_bound, std::string upper_bound,
                               std::function<bool(std::string)> key_filter = nullptr,
                               std::function<bool(T)> value_filter = nullptr) {
    auto ret = elements_->GetRangeValues(values, lower_bound, upper_bound, key_filter, value_filter);
    if (ret < 0) {
      return butil::Status(pb::error::EINTERNAL, "Meta get range values failed");
    }
    return butil::Status::OK();
  }

  MetaMemMapStd(const MetaMemMapStd &) = delete;
  const MetaMemMapStd &operator=(const MetaMemMapStd &) = delete;

 private:
  std::shared_ptr<RawEngine> raw_engine_;
  DingoSafeStdMap<std::string, T> *elements_;
};

}  // namespace dingodb

#endif  // DINGODB_COORDINATOR_META_STORAGE_H_