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

#include "scan/scan_filter.h"

namespace dingodb {

ScanFilter::ScanFilter(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc)
    : key_only_(key_only),
      max_fetch_cnt_(max_fetch_cnt),
      max_bytes_rpc_(max_bytes_rpc),
      cur_fetch_cnt_(0),
      cur_bytes_rpc_(0) {}

bool ScanFilter::UptoLimit(const pb::common::KeyValue& kv) {
  cur_fetch_cnt_++;
  if (cur_fetch_cnt_ >= max_fetch_cnt_) {
    return true;
  }

  cur_bytes_rpc_ += kv.key().size();
  if (!key_only_) {
    cur_bytes_rpc_ += kv.value().size();
  }

  if (cur_bytes_rpc_ >= max_bytes_rpc_) {
    return true;
  }

  return false;
}

void ScanFilter::Reset() {
  cur_fetch_cnt_ = 0;
  cur_bytes_rpc_ = 0;
}

void ScanFilter::Reset(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc) {
  key_only_ = key_only;
  max_fetch_cnt_ = max_fetch_cnt;
  max_bytes_rpc_ = max_bytes_rpc;
  cur_fetch_cnt_ = 0;
  cur_bytes_rpc_ = 0;
}
}  // namespace dingodb
