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

#include "butil/status.h"
#include "common/helper.h"
#include "mvcc/codec.h"

namespace dingodb {

namespace mvcc {

bool Iterator::Valid() const { return iter_->Valid(); }

void Iterator::SeekToFirst() { iter_->SeekToFirst(); }

void Iterator::SeekToLast() { iter_->SeekToLast(); }

void Iterator::Seek(const std::string& target) {
  now_time_ = Helper::TimestampMs();
  iter_->Seek(target);
  NextVisibleKey();
}

void Iterator::SeekForPrev(const std::string& target) {
  now_time_ = Helper::TimestampMs();
  iter_->SeekForPrev(target);
  PrevVisibleKey();
}

void Iterator::Next() { NextVisibleKey(); }

void Iterator::Prev() { PrevVisibleKey(); }

std::string_view Iterator::Key() const { return iter_->Key(); }

std::string_view Iterator::Value() const { return iter_->Value(); }

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
    std::cout << "ts_: " << ts_ << " ts: " << ts << std::endl;
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
  for (;;) {
    iter_->Prev();
    if (!iter_->Valid()) {
      break;
    }

    auto key = iter_->Key();
    int64_t ts = Codec::TruncateKeyForTs(key);
    if (ts > ts_) {
      continue;
    }

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

}  // namespace mvcc

}  // namespace dingodb