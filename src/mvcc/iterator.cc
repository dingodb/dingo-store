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

#include "mvcc/iterator.h"

#include <cstdint>
#include <string>

#include "butil/status.h"
#include "common/helper.h"
#include "glog/logging.h"
#include "mvcc/codec.h"

namespace dingodb {

namespace mvcc {

bool Iterator::Valid() const {
  CHECK(type_ != Type::kNone) << "Not already seek.";

  if (type_ == Type::kBackward) {
    return iter_->Valid();
  } else {
    return !key_.empty();
  }
}

void Iterator::SeekToFirst() { iter_->SeekToFirst(); }

void Iterator::SeekToLast() { iter_->SeekToLast(); }

void Iterator::Seek(const std::string& target) {
  CHECK(type_ == Type::kNone) << "can't repeat seek.";

  type_ = Type::kBackward;
  now_time_ = Helper::TimestampMs();
  iter_->Seek(target);
  NextVisibleKey();
}

void Iterator::SeekForPrev(const std::string& target) {
  CHECK(type_ == Type::kNone) << "can't repeat seek.";

  type_ = Type::kForward;
  now_time_ = Helper::TimestampMs();
  iter_->SeekForPrev(target);
  PrevVisibleKey();
}

void Iterator::Next() {
  CHECK(type_ == Type::kBackward) << "not match seek type.";

  NextVisibleKey();
}

void Iterator::Prev() {
  CHECK(type_ == Type::kForward) << "not match seek type.";

  PrevVisibleKey();
}

std::string_view Iterator::Key() const {
  CHECK(type_ != Type::kNone) << "Not already seek.";

  return type_ == Type::kBackward ? iter_->Key() : key_;
}

std::string_view Iterator::Value() const {
  CHECK(type_ != Type::kNone) << "Not already seek.";

  return type_ == Type::kBackward ? iter_->Value() : value_;
}

butil::Status Iterator::Status() const { return butil::Status::OK(); }

// filter condition:
// 1. key > ts
// 2. deleted key
// 3. ttl expires
void Iterator::NextVisibleKey() {
  for (; iter_->Valid(); iter_->Next()) {
    auto key = iter_->Key();
    auto encode_key = Codec::TruncateTsForKey(key);
    if (encode_key == prev_encode_key_) {
      continue;
    }

    int64_t ts = Codec::TruncateKeyForTs(key);
    if (ts > ts_) {
      continue;
    }

    prev_encode_key_ = encode_key;

    auto value = iter_->Value();
    auto flag = Codec::GetValueFlag(value);
    if (flag == ValueFlag::kDelete) {
      continue;

    } else if (flag == ValueFlag::kPutTTL) {
      int64_t ttl = Codec::GetValueTTL(value);
      if (ttl < now_time_) {
        continue;
      }
    }

    break;
  }
}

// filter condition:
// 1. key > ts
// 2. deleted key
// 3. ttl expires
void Iterator::PrevVisibleKey() {
  std::string prev_key;
  std::string prev_value;
  for (; iter_->Valid(); iter_->Prev()) {
    auto key = iter_->Key();

    auto encode_key = Codec::TruncateTsForKey(key);
    if (!prev_key.empty()) {
      auto prev_encode_key = Codec::TruncateTsForKey(prev_key);
      if (encode_key != prev_encode_key) {
        break;
      }
    }

    int64_t ts = Codec::TruncateKeyForTs(key);
    if (ts > ts_) {
      continue;
    }

    auto value = iter_->Value();
    auto flag = Codec::GetValueFlag(value);
    if (flag == ValueFlag::kDelete) {
      prev_key.clear();
      prev_value.clear();
      continue;
    } else if (flag == ValueFlag::kPutTTL) {
      int64_t ttl = Codec::GetValueTTL(value);
      if (ttl < now_time_) {
        prev_key.clear();
        prev_value.clear();
        continue;
      }
    }

    prev_key = key;
    prev_value = value;
  }

  key_.swap(prev_key);
  value_.swap(prev_value);
}

}  // namespace mvcc

}  // namespace dingodb