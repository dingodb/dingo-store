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

#include "log/index_term_map.h"

#include <algorithm>

#include "common/logging.h"
#include "fmt/format.h"

namespace dingodb {

IndexTermMap::IndexTermMap(int64_t region_id) : region_id_(region_id){};
IndexTermMap::~IndexTermMap() = default;

// Get term the log at |index|, return the term exactly or 0 if it's unknown
int64_t IndexTermMap::GetTerm(int64_t index) const {
  if (term_cache_.empty()) {
    return 0;
  }
  if (index >= term_cache_.back().index) {
    return term_cache_.back().term;
  }
  if (term_cache_.size() < 15ul /*FIXME: it's not determined by benchmark*/) {
    // In most case it's true, term doesn't change frequently.
    for (std::deque<braft::LogId>::const_reverse_iterator iter = term_cache_.rbegin(); iter != term_cache_.rend();
         ++iter) {
      if (index >= iter->index) {
        return iter->term;
      }
    }
  } else {
    std::deque<braft::LogId>::const_iterator iter =
        std::upper_bound(term_cache_.begin(), term_cache_.end(), index, Cmp());
    if (iter == term_cache_.begin()) {
      return 0;
    }
    --iter;
    return iter->term;
  }
  // The term of |index| is unknown
  return 0;
}

int IndexTermMap::Append(const braft::LogId& log_id) {
  if (term_cache_.empty()) {
    term_cache_.push_back(log_id);
    return 0;
  }
  if (log_id.index <= term_cache_.back().index || log_id.term < term_cache_.back().term) {
    DINGO_LOG(ERROR) << fmt::format(
        "region_id:{} Invalid log_id={}:{} while term_cache_.back()={}:{}"
        " do you forget to call TruncateSuffix() or Reset()",
        region_id_, log_id.index, log_id.term, term_cache_.back().index, term_cache_.back().term);
    return -1;
  }
  if (log_id.term != term_cache_.back().term) {
    term_cache_.push_back(log_id);
  }
  return 0;
}

void IndexTermMap::TruncatePrefix(int64_t first_index_kept) {
  if (term_cache_.empty()) {
    // TODO: Print log if it's an exception
    DINGO_LOG(WARNING) << fmt::format("region_id:{} term map has no logid, first_index_kept:{}", region_id_,
                                      first_index_kept);
    return;
  }
  size_t num_pop = 0;
  for (std::deque<braft::LogId>::const_iterator iter = term_cache_.begin() + 1; iter != term_cache_.end(); ++iter) {
    if (iter->index <= first_index_kept) {
      ++num_pop;
    } else {
      break;
    }
  }
  term_cache_.erase(term_cache_.begin(), term_cache_.begin() + num_pop);
}

void IndexTermMap::TruncateSuffix(int64_t last_index_kept) {
  while (!term_cache_.empty() && term_cache_.back().index > last_index_kept) {
    term_cache_.pop_back();
  }
}

void IndexTermMap::Reset() { term_cache_.clear(); }

}  // namespace dingodb