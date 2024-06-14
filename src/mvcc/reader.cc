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
#include "mvcc/codec.h"
#include "mvcc/iterator.h"

namespace dingodb {

namespace mvcc {

butil::Status Reader::KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) {
  if (key.empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::string encode_key = Codec::EncodeBytes(key);

  dingodb::IteratorOptions options;
  options.upper_bound = Helper::PrefixNext(encode_key);

  int64_t ts = ctx->Ts() > 0 ? ctx->Ts() : INT64_MAX;
  auto iter = std::make_shared<mvcc::Iterator>(ts, reader_->NewIterator(ctx->CfName(), options));
  iter->Seek(encode_key);
  if (!iter->Valid()) {
    return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
  }

  value = Codec::UnPackageValue(iter->Value());

  return butil::Status().OK();
}

butil::Status Reader::KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(start_key);
  std::string encode_end_key = Codec::EncodeBytes(end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  int64_t ts = ctx->Ts() > 0 ? ctx->Ts() : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(ctx->CfName(), options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    pb::common::KeyValue kv;

    auto key = iter->Key();

    std::string decode_key;
    int64_t ts = 0;
    Codec::DecodeKey(key, decode_key, ts);

    kv.set_ts(ts);
    kv.set_key(decode_key);
    kv.set_value(std::string(Codec::UnPackageValue(iter->Value())));

    kvs.push_back(std::move(kv));
  }

  return butil::Status().OK();
}

butil::Status Reader::KvCount(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                              int64_t& count) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "Start key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    return butil::Status(pb::error::EKEY_EMPTY, "End key is empty");
  }

  std::string encode_start_key = Codec::EncodeBytes(start_key);
  std::string encode_end_key = Codec::EncodeBytes(end_key);

  dingodb::IteratorOptions options;
  options.upper_bound = encode_end_key;

  int64_t ts = ctx->Ts() > 0 ? ctx->Ts() : INT64_MAX;
  auto iter = std::make_shared<Iterator>(ts, reader_->NewIterator(ctx->CfName(), options));
  for (iter->Seek(encode_start_key); iter->Valid(); iter->Next()) {
    ++count;
  }

  return butil::Status().OK();
}

dingodb::IteratorPtr Reader::NewIterator(const std::string& cf_name, int64_t ts, IteratorOptions options) {
  return std::make_shared<Iterator>(ts, reader_->NewIterator(cf_name, options));
}

}  // namespace mvcc

}  // namespace dingodb