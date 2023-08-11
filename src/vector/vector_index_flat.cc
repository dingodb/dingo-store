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

#include "vector/vector_index_flat.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "bthread/types.h"
#include "butil/status.h"
#include "common/logging.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "fmt/core.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/region_control.pb.h"
#include "vector/vector_index_filter.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

// Filter vecotr id used by region range.
class FlatRangeFilterFunctor : public faiss::IDSelector {
 public:
  FlatRangeFilterFunctor(std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) : filters_(filters) {}
  ~FlatRangeFilterFunctor() override = default;
  bool is_member(faiss::idx_t id) const override {  // NOLINT
    if (filters_.empty()) {
      return true;
    }
    for (const auto& filter : filters_) {
      if (!filter->Check(id)) {
        return false;
      }
    }

    return true;
  }

 private:
  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters_;
};

VectorIndexFlat::VectorIndexFlat(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
    : VectorIndex(id, vector_index_parameter, 0) {
  bthread_mutex_init(&mutex_, nullptr);

  metric_type_ = vector_index_parameter.flat_parameter().metric_type();
  dimension_ = vector_index_parameter.flat_parameter().dimension();

  normalize_ = false;

  if (pb::common::MetricType::METRIC_TYPE_L2 == metric_type_) {
    raw_index_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
  } else if (pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT == metric_type_) {
    raw_index_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
  } else if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
    raw_index_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
  } else {
    DINGO_LOG(WARNING) << fmt::format("Flat : not support metric type : {} use L2 default",
                                      static_cast<int>(metric_type_));
    raw_index_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
  }

  index_ = std::make_unique<faiss::IndexIDMap2>(raw_index_.get());
}

VectorIndexFlat::~VectorIndexFlat() {
  index_->reset();
  bthread_mutex_destroy(&mutex_);
}

// const float kFloatAccuracy = 0.00001;

// void NormalizeVec(float* x, int32_t d) {
//   float norm_l2_sqr = faiss::fvec_norm_L2sqr(x, d);
//   if (norm_l2_sqr > 0 && std::abs(1.0f - norm_l2_sqr) > kFloatAccuracy) {
//     float norm_l2 = std::sqrt(norm_l2_sqr);
//     for (int32_t i = 0; i < d; i++) {
//       x[i] = x[i] / norm_l2;
//     }
//   }
// }

butil::Status VectorIndexFlat::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                           bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status::OK();
  }

  // check
  uint32_t input_dimension = vector_with_ids[0].vector().float_values_size();
  if (input_dimension != static_cast<size_t>(dimension_)) {
    std::string s =
        fmt::format("Flat : float size : {} not equal to  dimension(create) : {}", input_dimension, dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  std::unique_ptr<faiss::idx_t[]> ids;
  try {
    ids.reset(new faiss::idx_t[vector_with_ids.size()]);
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for ids: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, "Failed to allocate memory for ids, error: %s", e.what());
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    ids.get()[i] = static_cast<faiss::idx_t>(vector_with_ids[i].id());
  }

  BAIDU_SCOPED_LOCK(mutex_);

  if (is_upsert) {
    faiss::IDSelectorArray sel(vector_with_ids.size(), ids.get());
    index_->remove_ids(sel);
  }

  std::unique_ptr<float[]> vectors;
  try {
    vectors.reset(new float[vector_with_ids.size() * dimension_]);
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for vectors: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, "Failed to allocate memory for vectors, error: %s",
                         e.what());
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    const auto& vector = vector_with_ids[i].vector().float_values();
    memcpy(vectors.get() + i * dimension_, vector.data(), dimension_ * sizeof(float));

    if (normalize_) {
      VectorIndexUtils::NormalizeVectorForFaiss(vectors.get() + i * dimension_, dimension_);
    }
  }

  index_->add_with_ids(vector_with_ids.size(), vectors.get(), ids.get());

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsert(vector_with_ids, true);
}

butil::Status VectorIndexFlat::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsert(vector_with_ids, false);
}

butil::Status VectorIndexFlat::Delete(const std::vector<uint64_t>& delete_ids) {
  std::unique_ptr<faiss::idx_t[]> ids;
  try {
    ids.reset(new faiss::idx_t[delete_ids.size()]);
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for ids: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, "Failed to allocate memory for ids, error: %s", e.what());
  }

  for (size_t i = 0; i < delete_ids.size(); ++i) {
    ids.get()[i] = static_cast<faiss::idx_t>(delete_ids[i]);
  }

  faiss::IDSelectorArray sel(delete_ids.size(), ids.get());

  size_t remove_count = 0;
  {
    BAIDU_SCOPED_LOCK(mutex_);
    remove_count = index_->remove_ids(sel);
  }

  if (0 == remove_count) {
    DINGO_LOG(ERROR) << fmt::format("not found id : {}", id);
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, fmt::format("not found : {}", id));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                      std::vector<std::shared_ptr<FilterFunctor>> filters,
                                      std::vector<pb::index::VectorWithDistanceResult>& results,
                                      [[maybe_unused]] bool reconstruct) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  uint32_t amplify_topk = topk;
  if (!filters.empty()) {
    amplify_topk = topk * 4;
  }

  if (vector_with_ids[0].vector().float_values_size() != this->dimension_) {
    std::string s = fmt::format("vector dimension is not match, input = {}, index = {} ",
                                vector_with_ids[0].vector().float_values_size(), this->dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(amplify_topk * vector_with_ids.size(), 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(amplify_topk * vector_with_ids.size(), -1);

  std::unique_ptr<float[]> vectors;
  try {
    vectors.reset(new float[vector_with_ids.size() * dimension_]);
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for vectors: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, "Failed to allocate memory for vectors, error: %s",
                         e.what());
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    if (vector_with_ids[i].vector().float_values_size() != this->dimension_) {
      DINGO_LOG(ERROR) << fmt::format(
          "vector dimension is not equal to index dimension, vector id : {}, float_value_zie: {}, index dimension: {}",
          vector_with_ids[i].id(), vector_with_ids[i].vector().float_values_size(), this->dimension_);

      return butil::Status(
          pb::error::Errno::EVECTOR_INVALID,
          fmt::format("vector dimension is not equal to index dimension, vector id : {}, "
                      "float_value_zie: {}, index dimension: {}",
                      vector_with_ids[i].id(), vector_with_ids[i].vector().float_values_size(), this->dimension_));
    } else {
      const auto& vector = vector_with_ids[i].vector().float_values();
      memcpy(vectors.get() + i * dimension_, vector.data(), dimension_ * sizeof(float));

      if (normalize_) {
        VectorIndexUtils::NormalizeVectorForFaiss(vectors.get() + i * dimension_, dimension_);
      }
    }
  }

  {
    bool enable_filter = (!vector_ids.empty());

    std::vector<uint64_t> internal_vector_ids;
    internal_vector_ids.reserve(vector_ids.size());
    for (auto vector_id : vector_ids) {
      auto iter = index_->rev_map.find(static_cast<faiss::idx_t>(vector_id));
      if (iter == index_->rev_map.end()) {
        DINGO_LOG(WARNING) << fmt::format("vector_id : {} not exist . ignore", vector_id);
        continue;
      }
      internal_vector_ids.push_back(iter->second);
    }

    // if all original id not found. ignore .result nothing
    // if (internal_vector_ids.empty()) {
    //   enable_filter = false;
    // }

    std::unique_ptr<SearchFilterForFlat> search_filter_for_flat_ptr =
        std::make_unique<SearchFilterForFlat>(std::move(internal_vector_ids));
    faiss::SearchParameters* param = enable_filter ? search_filter_for_flat_ptr.get() : nullptr;

    BAIDU_SCOPED_LOCK(mutex_);
    index_->search(vector_with_ids.size(), vectors.get(), amplify_topk, distances.data(), labels.data(), nullptr);
  }

  for (size_t row = 0; row < vector_with_ids.size(); ++row) {
    auto& result = results.emplace_back();

    int count = 0;
    for (size_t i = 0; i < amplify_topk; i++) {
      size_t pos = row * amplify_topk + i;
      if (labels[pos] < 0) {
        continue;
      }

      // filter invalid value
      if (!filters.empty()) {
        bool valid = true;
        for (auto& filter : filters) {
          if (!filter->Check(labels[pos])) {
            valid = false;
            break;
          }
        }
        if (!valid) {
          continue;
        }
      }
      ++count;

      auto* vector_with_distance = result.add_vector_with_distances();

      auto* vector_with_id = vector_with_distance->mutable_vector_with_id();
      vector_with_id->set_id(labels[pos]);
      vector_with_id->mutable_vector()->set_dimension(dimension_);
      vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      if (metric_type_ == pb::common::MetricType::METRIC_TYPE_COSINE ||
          metric_type_ == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
        vector_with_distance->set_distance(1.0F - distances[pos]);
      } else {
        vector_with_distance->set_distance(distances[pos]);
      }

      vector_with_distance->set_metric_type(metric_type_);
      if (!filters.empty() && count == topk) {
        break;
      }
    }
  }

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

void VectorIndexFlat::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexFlat::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

butil::Status VectorIndexFlat::Save(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Flat index not support save");
}

butil::Status VectorIndexFlat::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Flat index not support load");
}

int32_t VectorIndexFlat::GetDimension() { return this->dimension_; }

butil::Status VectorIndexFlat::GetCount(uint64_t& count) {
  count = index_->id_map.size();
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetDeletedCount(uint64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetMemorySize(uint64_t& memory_size) {
  auto count = index_->ntotal;
  if (count == 0) {
    memory_size = 0;
    return butil::Status::OK();
  }

  memory_size = count * sizeof(faiss::idx_t) + count * dimension_ * sizeof(faiss::Index::component_t) +
                (sizeof(faiss::idx_t) + sizeof(faiss::idx_t)) * index_->rev_map.size();
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::NeedToRebuild(bool& need_to_rebuild, [[maybe_unused]] uint64_t last_save_log_behind) {
  need_to_rebuild = false;
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::NeedToSave(bool& need_to_save, [[maybe_unused]] uint64_t last_save_log_behind) {
  need_to_save = false;
  return butil::Status::OK();
}

void VectorIndexFlat::SearchWithParam(faiss::idx_t n, const faiss::Index::component_t* x, faiss::idx_t k,
                                      faiss::Index::distance_t* distances, faiss::idx_t* labels,
                                      const faiss::SearchParameters* params) {
  raw_index_->search(n, x, k, distances, labels, params);
  faiss::idx_t* li = labels;
#pragma omp parallel for
  for (faiss::idx_t i = 0; i < n * k; i++) {
    li[i] = li[i] < 0 ? li[i] : index_->id_map[li[i]];
  }
}

}  // namespace dingodb
