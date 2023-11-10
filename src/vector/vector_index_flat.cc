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
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "bthread/types.h"
#include "butil/status.h"
#include "common/logging.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "faiss/index_io.h"
#include "fmt/core.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/region_control.pb.h"
#include "vector/vector_index_utils.h"

namespace dingodb {
DEFINE_uint64(flat_need_save_count, 10000, "flat need save count");

VectorIndexFlat::VectorIndexFlat(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                 const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, range) {
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

  index_id_map2_ = std::make_unique<faiss::IndexIDMap2>(raw_index_.get());
}

VectorIndexFlat::~VectorIndexFlat() {
  index_id_map2_->reset();
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

butil::Status VectorIndexFlat::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                  bool is_upsert) {
  return AddOrUpsert(vector_with_ids, is_upsert);
}

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

  std::thread([&]() {
    if (is_upsert) {
      faiss::IDSelectorArray sel(vector_with_ids.size(), ids.get());
      index_id_map2_->remove_ids(sel);
    }
    index_id_map2_->add_with_ids(vector_with_ids.size(), vectors.get(), ids.get());
  }).join();

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexFlat::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexFlat::Delete(const std::vector<int64_t>& delete_ids) {
  if (delete_ids.empty()) {
    DINGO_LOG(WARNING) << "delete ids is empty";
    return butil::Status::OK();
  }

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
    std::thread([&]() { remove_count = index_id_map2_->remove_ids(sel); }).join();
  }

  if (0 == remove_count) {
    DINGO_LOG(ERROR) << fmt::format("not found id : {}", id);
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, fmt::format("not found : {}", id));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                      std::vector<std::shared_ptr<FilterFunctor>> filters,
                                      std::vector<pb::index::VectorWithDistanceResult>& results, bool /*reconstruct*/,
                                      const pb::common::VectorSearchParameter& /*parameter*/) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  if (vector_with_ids[0].vector().float_values_size() != this->dimension_) {
    std::string s = fmt::format("vector dimension is not match, input = {}, index = {} ",
                                vector_with_ids[0].vector().float_values_size(), this->dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(topk * vector_with_ids.size(), 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk * vector_with_ids.size(), -1);

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

  faiss::SearchParameters flat_search_parameters;

  {
    BAIDU_SCOPED_LOCK(mutex_);
    // use std::thread to call faiss functions
    std::thread t([&]() {
      if (!filters.empty()) {
        //   // Build array index list.
        //   for (auto& filter : filters) {
        //     filter->Build(index_id_map2_->id_map);
        //   }
        //   auto flat_filter = filters.empty() ? nullptr : std::make_shared<FlatIDSelector>(filters);
        //   SearchWithParam(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(), flat_filter);

        // use faiss's search_param to do pre-filter
        auto flat_filter = filters.empty() ? nullptr : std::make_shared<FlatIDSelector>(filters);
        flat_search_parameters.sel = flat_filter.get();
        index_id_map2_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(),
                               &flat_search_parameters);
      } else {
        index_id_map2_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data());
      }
    });
    t.join();
  }

  for (size_t row = 0; row < vector_with_ids.size(); ++row) {
    auto& result = results.emplace_back();

    for (size_t i = 0; i < topk; i++) {
      size_t pos = row * topk + i;
      if (labels[pos] < 0) {
        continue;
      }
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
    }
  }

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

void VectorIndexFlat::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexFlat::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

bool VectorIndexFlat::SupportSave() { return true; }

butil::Status VectorIndexFlat::Save(const std::string& path) {
  if (BAIDU_UNLIKELY(path.empty())) {
    std::string s = fmt::format("path empty. not support");
    // DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  // The outside has been locked. Remove the locking operation here.
  // BAIDU_SCOPED_LOCK(mutex_);
  std::promise<butil::Status> promise_status;
  std::future<butil::Status> future_status = promise_status.get_future();
  std::thread t(
      [&](std::promise<butil::Status>& promise_status) {
        try {
          faiss::write_index(index_id_map2_.get(), path.c_str());
          promise_status.set_value(butil::Status());
        } catch (std::exception& e) {
          std::string s =
              fmt::format("VectorIndexFlat::Save faiss::write_index failed. path : {} error : {}", path, e.what());
          // LOG(ERROR) << "["
          //            << "lambda faiss::write_index"
          //            << "] " << s;
          promise_status.set_value(butil::Status(pb::error::Errno::EINTERNAL, s));
        }
      },
      std::ref(promise_status));

  butil::Status status = future_status.get();
  t.join();

  if (status.ok()) {
    // DINGO_LOG(INFO) << fmt::format("VectorIndexFlat::Save success. path : {}", path);
  }

  return status;
}

butil::Status VectorIndexFlat::Load(const std::string& path) {
  if (BAIDU_UNLIKELY(path.empty())) {
    std::string s = fmt::format("path empty. not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  // The outside has been locked. Remove the locking operation here.
  // BAIDU_SCOPED_LOCK(mutex_);
  std::promise<std::pair<faiss::Index*, butil::Status>> promise_status;
  std::future<std::pair<faiss::Index*, butil::Status>> future_status = promise_status.get_future();
  std::thread t(
      [&](std::promise<std::pair<faiss::Index*, butil::Status>>& promise_status) {
        faiss::Index* internal_raw_index = nullptr;
        try {
          faiss::Index* internal_raw_index = faiss::read_index(path.c_str(), 0);
          promise_status.set_value(std::pair<faiss::Index*, butil::Status>(internal_raw_index, butil::Status()));
        } catch (std::exception& e) {
          std::string s =
              fmt::format("VectorIndexFlat::Load faiss::read_index failed. path : {} error : {}", path, e.what());
          LOG(ERROR) << "["
                     << "lambda faiss::read_index"
                     << "] " << s;
          promise_status.set_value(std::pair<faiss::Index*, butil::Status>(
              internal_raw_index, butil::Status(pb::error::Errno::EINTERNAL, s)));
        }
      },
      std::ref(promise_status));

  auto [internal_raw_index, status] = future_status.get();
  t.join();

  if (!status.ok()) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }

    return status;
  }

  faiss::IndexIDMap2* internal_index = dynamic_cast<faiss::IndexIDMap2*>(internal_raw_index);
  if (BAIDU_UNLIKELY(!internal_index)) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }
    std::string s =
        fmt::format("VectorIndexFlat::Load faiss::read_index failed. Maybe not IndexIVFPq.  path : {} ", path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // avoid mem leak!!!
  std::unique_ptr<faiss::IndexIDMap2> internal_index_id_map2(internal_index);

  // double check
  if (BAIDU_UNLIKELY(internal_index->d != dimension_)) {
    std::string s = fmt::format("VectorIndexFlat::Load load dimension : {} !=  dimension_ : {}. path : {}",
                                internal_index->d, dimension_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (BAIDU_UNLIKELY(!internal_index->is_trained)) {
    std::string s = fmt::format("VectorIndexFlat::Load load is not train. path : {}", path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  switch (metric_type_) {
    case pb::common::METRIC_TYPE_NONE:
      [[fallthrough]];
    case pb::common::METRIC_TYPE_L2: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_L2)) {
        std::string s =
            fmt::format("VectorIndexFlat::Load load from path type : {} != local type : {}. path : {}",
                        static_cast<int>(internal_index->metric_type), static_cast<int>(metric_type_), path);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }

    case pb::common::METRIC_TYPE_INNER_PRODUCT:
    case pb::common::METRIC_TYPE_COSINE: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_INNER_PRODUCT)) {
        std::string s =
            fmt::format("VectorIndexFlat::Load load from path type : {} != local type : {}. path : {}",
                        static_cast<int>(internal_index->metric_type), static_cast<int>(metric_type_), path);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }
    case pb::common::MetricType_INT_MIN_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    case pb::common::MetricType_INT_MAX_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("VectorIndexFlat::Load load from path type : {} != local type : {}. path : {}",
                                  static_cast<int>(internal_index->metric_type), static_cast<int>(metric_type_), path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  raw_index_.reset();
  index_id_map2_ = std::move(internal_index_id_map2);

  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  DINGO_LOG(INFO) << fmt::format("VectorIndexFlat::Load success. path : {}", path);

  return butil::Status::OK();
}

int32_t VectorIndexFlat::GetDimension() { return this->dimension_; }

butil::Status VectorIndexFlat::GetCount(int64_t& count) {
  BAIDU_SCOPED_LOCK(mutex_);
  count = index_id_map2_->id_map.size();
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetMemorySize(int64_t& memory_size) {
  BAIDU_SCOPED_LOCK(mutex_);
  auto count = index_id_map2_->ntotal;
  if (count == 0) {
    memory_size = 0;
    return butil::Status::OK();
  }

  memory_size = count * sizeof(faiss::idx_t) + count * dimension_ * sizeof(faiss::Index::component_t) +
                (sizeof(faiss::idx_t) + sizeof(faiss::idx_t)) * index_id_map2_->rev_map.size();
  return butil::Status::OK();
}

bool VectorIndexFlat::IsExceedsMaxElements() { return false; }

void VectorIndexFlat::SearchWithParam(faiss::idx_t n, const faiss::Index::component_t* x, faiss::idx_t k,
                                      faiss::Index::distance_t* distances, faiss::idx_t* labels,
                                      std::shared_ptr<FlatIDSelector> filters) {
  faiss::SearchParameters param;
  param.sel = filters.get();
  raw_index_->search(n, x, k, distances, labels, &param);
  faiss::idx_t* li = labels;
  // TODO: running by regular user may cause abort exception below
  // libgomp: Thread creation failed: Resource temporarily unavailable
  // please run by root, or adjust ulimit for regular user, then you can increase FLAGS_omp_num_threads
#pragma omp parallel for
  for (faiss::idx_t i = 0; i < n * k; ++i) {
    li[i] = li[i] < 0 ? li[i] : index_id_map2_->id_map[li[i]];
  }
}

bool VectorIndexFlat::NeedToSave(int64_t last_save_log_behind) {
  BAIDU_SCOPED_LOCK(mutex_);

  int64_t element_count = 0;

  element_count = index_id_map2_->id_map.size();

  if (element_count == 0) {
    return false;
  }

  if (last_save_log_behind > FLAGS_flat_need_save_count) {
    return true;
  }

  return false;
}

}  // namespace dingodb
