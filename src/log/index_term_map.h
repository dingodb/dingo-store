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

// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.

#ifndef DINGODB_LOG_INDEX_TERM_MAP_H_
#define DINGODB_LOG_INDEX_TERM_MAP_H_

#include <cstdint>
#include <deque>

#include "braft/log_entry.h"

namespace dingodb {

class IndexTermMap {
 public:
  explicit IndexTermMap(int64_t region_id);
  ~IndexTermMap();

  struct Cmp {
    bool operator()(int64_t index, const braft::LogId& log_id) const { return index < log_id.index; }
  };

  // Get term the log at |index|, return the term exactly or 0 if it's unknown
  int64_t GetTerm(int64_t index) const;

  int Append(const braft::LogId& log_id);

  void TruncatePrefix(int64_t first_index_kept);

  void TruncateSuffix(int64_t last_index_kept);

  void Reset();

 private:
  std::deque<braft::LogId> term_cache_;
  int64_t region_id_;
};

}  // namespace dingodb

#endif  // DINGODB_LOG_INDEX_TERM_MAP_H_