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

#include "vector/vector_index_filter.h"

#include <utility>

namespace dingodb {

SearchFilterForFlat::SearchFilterForFlat(const std::vector<uint64_t>& vector_ids) {  // NOLINT
  ids.reserve(vector_ids.size());

  for (auto vector_id : vector_ids) {
    ids.push_back(static_cast<faiss::idx_t>(vector_id));
  }

  selector_ptr = std::make_shared<faiss::IDSelectorArray>(ids.size(), ids.data());
  sel = selector_ptr.get();
}

SearchFilterForFlat::SearchFilterForFlat(std::vector<faiss::idx_t>&& vector_ids) {  // NOLINT
  ids = std::forward<std::vector<faiss::idx_t>>(vector_ids);
  selector_ptr = std::make_shared<faiss::IDSelectorArray>(ids.size(), ids.data());
  sel = selector_ptr.get();
}

SearchFilterForHnsw::SearchFilterForHnsw(const std::vector<uint64_t>& vector_ids) {
  ids_.reserve(vector_ids.size());

  for (auto vector_id : vector_ids) {
    ids_.push_back(static_cast<hnswlib::labeltype>(vector_id));
  }
}

bool SearchFilterForHnsw::operator()(hnswlib::labeltype id) {
  for (auto elem : ids_) {
    if (id == elem) {
      return true;
    }
  }
  return false;
}

}  // namespace dingodb
