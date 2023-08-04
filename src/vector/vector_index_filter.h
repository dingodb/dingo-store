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

#ifndef DINGODB_VECTOR_INDEX_FILTER_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_FILTER_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "faiss/Index.h"
#include "faiss/impl/IDSelector.h"
#include "hnswlib/hnswlib.h"

namespace dingodb {

struct SearchFilterForFlat : public faiss::SearchParameters {
  SearchFilterForFlat(const SearchFilterForFlat&) = delete;
  SearchFilterForFlat(SearchFilterForFlat&&) = delete;
  SearchFilterForFlat& operator=(const SearchFilterForFlat&) = delete;
  SearchFilterForFlat& operator=(SearchFilterForFlat&&) = delete;

  [[deprecated("poor performance")]] explicit SearchFilterForFlat(const std::vector<uint64_t>& vector_ids);  // NOLINT

  explicit SearchFilterForFlat(std::vector<faiss::idx_t>&& vector_ids);

  ~SearchFilterForFlat() override = default;

  std::shared_ptr<faiss::IDSelector> selector_ptr;
  std::vector<faiss::idx_t> ids;
};

// be careful not to use the parent class to release,
// otherwise there will be memory leaks
class SearchFilterForHnsw : public hnswlib::BaseFilterFunctor {
 public:
  SearchFilterForHnsw(const SearchFilterForHnsw&) = delete;
  SearchFilterForHnsw(SearchFilterForHnsw&&) = delete;
  SearchFilterForHnsw& operator=(const SearchFilterForHnsw&) = delete;
  SearchFilterForHnsw& operator=(SearchFilterForHnsw&&) = delete;

  explicit SearchFilterForHnsw(const std::vector<uint64_t>& vector_ids);

  virtual ~SearchFilterForHnsw() = default;

  bool operator()(hnswlib::labeltype id) override;

 private:
  std::vector<hnswlib::labeltype> ids_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_FILTER_H_  // NOLINT
