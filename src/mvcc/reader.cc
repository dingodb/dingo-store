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

#include "mvcc/reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "mvcc/codec.h"
#include "mvcc/iterator.h"
#include "vector/codec.h"

namespace dingodb {

namespace mvcc {

butil::Status KvReader::KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                              std::string& plain_value) {
  if (plain_key.empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::string encode_key = Codec::EncodeBytes(plain_key);

  dingodb::IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(encode_key);

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<mvcc::Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->Seek(encode_key);
  if (!iter->Valid()) {
    return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
  }

  plain_value = Codec::UnPackageValue(iter->Value());

  return butil::Status().OK();
}

butil::Status KvReader::KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                               const std::string& plain_end_key, std::vector<pb::common::KeyValue>& plain_kvs) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    pb::common::KeyValue kv;

    auto key = iter->Key();

    std::string decode_key;
    int64_t ts = 0;
    Codec::DecodeKey(key, decode_key, ts);

    kv.set_ts(ts);
    kv.set_key(decode_key);
    kv.set_value(std::string(Codec::UnPackageValue(iter->Value())));

    plain_kvs.push_back(std::move(kv));
  }

  return butil::Status().OK();
}

butil::Status KvReader::KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                               const std::string& plain_end_key,
                               std::function<bool(const std::string& plain_key, const std::string& value)> func) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    std::string plain_key;
    int64_t ts = 0;
    Codec::DecodeKey(iter->Key(), plain_key, ts);

    std::string value(Codec::UnPackageValue(iter->Value()));

    if (!func(plain_key, value)) {
      break;
    }
  }

  return butil::Status().OK();
}

butil::Status KvReader::KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                const std::string& plain_end_key, int64_t& count) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  count = 0;
  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    ++count;
  }

  return butil::Status().OK();
}

butil::Status KvReader::KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                 const std::string& plain_end_key, std::string& plain_key) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  std::string min_key;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->Seek(encode_start_key);
  if (iter->Valid()) {
    int64_t ts;
    Codec::DecodeKey(iter->Key(), plain_key, ts);
  }

  return butil::Status().OK();
}

butil::Status KvReader::KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                 const std::string& plain_end_key, std::string& plain_key) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.lower_bound = encode_start_key;

  ts = ts > 0 ? ts : INT64_MAX;
  std::string min_key;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->SeekForPrev(encode_end_key);
  if (iter->Valid()) {
    int64_t ts;
    Codec::DecodeKey(iter->Key(), plain_key, ts);
  }

  return butil::Status().OK();
}

dingodb::IteratorPtr KvReader::NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) {
  return std::make_shared<Iterator>(ts > 0 ? ts : INT64_MAX, reader_->NewIterator(cf_name, options));
}

butil::Status VectorReader::KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                                  std::string& plain_value) {
  if (plain_key.empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::string encode_key = Codec::EncodeBytes(plain_key);

  dingodb::IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(encode_key);

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<mvcc::Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->Seek(encode_key);
  if (!iter->Valid()) {
    return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
  }

  plain_value = Codec::UnPackageValue(iter->Value());

  return butil::Status().OK();
}

// plain_start_key and plain_end_key is user key
// output plain_kvs is user key
butil::Status VectorReader::KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                   const std::string& plain_end_key, std::vector<pb::common::KeyValue>& plain_kvs) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    pb::common::KeyValue kv;

    std::string plain_key;
    int64_t ts;
    mvcc::Codec::DecodeKey(iter->Key(), plain_key, ts);

    kv.set_ts(ts);
    kv.set_key(plain_key);
    kv.set_value(std::string(Codec::UnPackageValue(iter->Value())));

    plain_kvs.push_back(std::move(kv));
  }

  return butil::Status().OK();
}

butil::Status VectorReader::KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                   const std::string& plain_end_key,
                                   std::function<bool(const std::string& plain_key, const std::string& value)> func) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    std::string plain_key;
    int64_t ts = 0;
    Codec::DecodeKey(iter->Key(), plain_key, ts);

    std::string value(Codec::UnPackageValue(iter->Value()));

    if (!func(plain_key, value)) {
      break;
    }
  }

  return butil::Status().OK();
}

// plain_start_key and plain_end_key is user key
butil::Status VectorReader::KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                    const std::string& plain_end_key, int64_t& count) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  count = 0;
  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    ++count;
  }

  return butil::Status().OK();
}

butil::Status VectorReader::KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                     const std::string& plain_end_key, std::string& plain_key) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  std::string min_key;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->Seek(encode_start_key);
  if (iter->Valid()) {
    int64_t ts;
    Codec::DecodeKey(iter->Key(), plain_key, ts);
  }

  return butil::Status().OK();
}

butil::Status VectorReader::KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                     const std::string& plain_end_key, std::string& plain_key) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.lower_bound = encode_start_key;

  ts = ts > 0 ? ts : INT64_MAX;
  std::string min_key;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->SeekForPrev(encode_end_key);
  if (iter->Valid()) {
    int64_t ts;
    Codec::DecodeKey(iter->Key(), plain_key, ts);
  }

  return butil::Status().OK();
}

dingodb::IteratorPtr VectorReader::NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) {
  return std::make_shared<Iterator>(ts > 0 ? ts : INT64_MAX, reader_->NewIterator(cf_name, options));
}

butil::Status DocumentReader::KvGet(const std::string& cf_name, int64_t ts, const std::string& plain_key,
                                    std::string& plain_value) {
  if (plain_key.empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::string encode_key = Codec::EncodeBytes(plain_key);

  dingodb::IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(encode_key);

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<mvcc::Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->Seek(encode_key);
  if (!iter->Valid()) {
    return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
  }

  plain_value = Codec::UnPackageValue(iter->Value());

  return butil::Status().OK();
}

// plain_start_key and plain_end_key is user key
// output plain_kvs is user key
butil::Status DocumentReader::KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                     const std::string& plain_end_key, std::vector<pb::common::KeyValue>& plain_kvs) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    pb::common::KeyValue kv;

    std::string plain_key;
    int64_t ts;
    mvcc::Codec::DecodeKey(iter->Key(), plain_key, ts);

    kv.set_ts(ts);
    kv.set_key(plain_key);
    kv.set_value(std::string(Codec::UnPackageValue(iter->Value())));

    plain_kvs.push_back(std::move(kv));
  }

  return butil::Status().OK();
}

butil::Status DocumentReader::KvScan(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                     const std::string& plain_end_key,
                                     std::function<bool(const std::string& plain_key, const std::string& value)> func) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    std::string plain_key;
    int64_t ts = 0;
    Codec::DecodeKey(iter->Key(), plain_key, ts);

    std::string value(Codec::UnPackageValue(iter->Value()));

    if (!func(plain_key, value)) {
      break;
    }
  }

  return butil::Status().OK();
}

// plain_start_key and plain_end_key is user key
butil::Status DocumentReader::KvCount(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                      const std::string& plain_end_key, int64_t& count) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  count = 0;
  ts = ts > 0 ? ts : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    ++count;
  }

  return butil::Status().OK();
}

butil::Status DocumentReader::KvMinKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                       const std::string& plain_end_key, std::string& plain_key) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  ts = ts > 0 ? ts : INT64_MAX;
  std::string min_key;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->Seek(encode_start_key);
  if (iter->Valid()) {
    int64_t ts;
    Codec::DecodeKey(iter->Key(), plain_key, ts);
  }

  return butil::Status().OK();
}

butil::Status DocumentReader::KvMaxKey(const std::string& cf_name, int64_t ts, const std::string& plain_start_key,
                                       const std::string& plain_end_key, std::string& plain_key) {
  if (BAIDU_UNLIKELY(plain_start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(plain_end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(plain_start_key);
  std::string encode_end_key = Codec::EncodeBytes(plain_end_key);

  dingodb::IteratorOptions options;
  options.lower_bound = encode_start_key;

  ts = ts > 0 ? ts : INT64_MAX;
  std::string min_key;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
  iter->SeekForPrev(encode_end_key);
  if (iter->Valid()) {
    int64_t ts;
    Codec::DecodeKey(iter->Key(), plain_key, ts);
  }

  return butil::Status().OK();
}

dingodb::IteratorPtr DocumentReader::NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) {
  return std::make_shared<Iterator>(ts > 0 ? ts : INT64_MAX, reader_->NewIterator(cf_name, options));
}

}  // namespace mvcc

}  // namespace dingodb