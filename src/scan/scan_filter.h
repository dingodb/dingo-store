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

#ifndef DINGODB_ENGINE_SCAN_FILTER_H_  // NOLINT
#define DINGODB_ENGINE_SCAN_FILTER_H_

#include <cstddef>
#include <cstdint>

#include "proto/common.pb.h"

namespace dingodb {

class ScanFilter {
 public:
  explicit ScanFilter(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc);
  ~ScanFilter() = default;
  ScanFilter(const ScanFilter& rhs) = default;
  ScanFilter& operator=(const ScanFilter& rhs) = default;
  ScanFilter(ScanFilter&& rhs) = default;
  ScanFilter& operator=(ScanFilter&& rhs) = default;

  bool UptoLimit(const pb::common::KeyValue& kv);

  void Reset();

  void Reset(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc);

  size_t GetCurFetchCnt() const { return cur_fetch_cnt_; }
  size_t GetCurBytesRpc() const { return cur_bytes_rpc_; }

 private:
  bool key_only_;
  size_t max_fetch_cnt_;
  int64_t max_bytes_rpc_;
  size_t cur_fetch_cnt_;
  int64_t cur_bytes_rpc_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_SCAN_FILTER_H_  // NOLINT
