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
#include "common/synchronization.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "faiss/impl/AuxIndexStructures.h"
#include "faiss/index_io.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/debug.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

DEFINE_int64(flat_need_save_count, 10000, "flat need save count");

VectorIndexFlat::VectorIndexFlat(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                 const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, epoch, range) {
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

VectorIndexFlat::~VectorIndexFlat() { index_id_map2_->reset(); }

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

  const auto& [ids, status_ids] = VectorIndexUtils::CheckAndCopyVectorId(vector_with_ids, dimension_);
  if (!status_ids.ok()) {
    DINGO_LOG(ERROR) << status_ids.error_cstr();
    return status_ids;
  }

  // fix lambda can not capture rvalue. change rvalue -> lvalue.
  // c++ 20 fix this bug.
  const std::unique_ptr<faiss::idx_t[]>& ids2 = ids;

  const auto& [vectors, status] = VectorIndexUtils::CopyVectorData(vector_with_ids, dimension_, normalize_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // fix lambda can not capture rvalue. change rvalue -> lvalue.
  // c++ 20 fix this bug.
  const std::unique_ptr<float[]>& vectors2 = vectors;

  RWLockWriteGuard guard(&rw_lock_);
  std::thread([&]() {
    if (is_upsert) {
      faiss::IDSelectorArray sel(vector_with_ids.size(), ids2.get());
      index_id_map2_->remove_ids(sel);
    }
    index_id_map2_->add_with_ids(vector_with_ids.size(), vectors2.get(), ids2.get());
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
    return butil::Status::OK();
  }

  const auto& [ids, status] = VectorIndexUtils::CopyVectorId(delete_ids);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  faiss::IDSelectorArray sel(delete_ids.size(), ids.get());

  size_t remove_count = 0;
  {
    RWLockWriteGuard guard(&rw_lock_);
    std::thread([&]() { remove_count = index_id_map2_->remove_ids(sel); }).join();
  }

  if (0 == remove_count) {
    DINGO_LOG(ERROR) << fmt::format("not found id : {}", id);
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, fmt::format("not found : {}", id));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                      std::vector<std::shared_ptr<FilterFunctor>> filters, bool,
                                      const pb::common::VectorSearchParameter&,
                                      std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(topk * vector_with_ids.size(), 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk * vector_with_ids.size(), -1);

  const auto& [vectors, status] = VectorIndexUtils::CheckAndCopyVectorData(vector_with_ids, dimension_, normalize_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // fix lambda can not capture rvalue. change rvalue -> lvalue.
  // c++ 20 fix this bug.
  const std::unique_ptr<float[]>& vectors2 = vectors;

  faiss::SearchParameters flat_search_parameters;

  {
    RWLockReadGuard guard(&rw_lock_);
    // use std::thread to call faiss functions
    std::thread t([&]() {
      if (!filters.empty()) {
        //   // Build array index list.
        //   for (auto& filter : filters) {
        //     filter->Build(index_id_map2_->id_map);
        //   }
        //   auto flat_filter = filters.empty() ? nullptr : std::make_shared<FlatIDSelector>(filters);
        //   SearchWithParam(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(),
        //   flat_filter);

        // use faiss's search_param to do pre-filter
        auto flat_filter = filters.empty() ? nullptr : std::make_shared<FlatIDSelector>(filters);
        flat_search_parameters.sel = flat_filter.get();
        index_id_map2_->search(vector_with_ids.size(), vectors2.get(), topk, distances.data(), labels.data(),
                               &flat_search_parameters);
      } else {
        index_id_map2_->search(vector_with_ids.size(), vectors2.get(), topk, distances.data(), labels.data());
      }
    });
    t.join();
  }

  VectorIndexUtils::FillSearchResult(vector_with_ids, topk, distances, labels, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::RangeSearch(std::vector<pb::common::VectorWithId> vector_with_ids, float radius,
                                           std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                           bool /*reconstruct*/, const pb::common::VectorSearchParameter& /*parameter*/,
                                           std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  const auto& [vectors, status] = VectorIndexUtils::CheckAndCopyVectorData(vector_with_ids, dimension_, normalize_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // fix lambda can not capture rvalue. change rvalue -> lvalue.
  // c++ 20 fix this bug.
  const std::unique_ptr<float[]>& vectors2 = vectors;

  std::unique_ptr<faiss::RangeSearchResult> range_search_result =
      std::make_unique<faiss::RangeSearchResult>(vector_with_ids.size());

  if (metric_type_ == pb::common::MetricType::METRIC_TYPE_COSINE ||
      metric_type_ == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
    radius = 1.0F - radius;
  }

  {
    RWLockReadGuard guard(&rw_lock_);
    // use std::thread to call faiss functions
    std::promise<butil::Status> promise_status;
    std::future<butil::Status> future_status = promise_status.get_future();
    // use std::thread to call faiss functions
    std::thread t(
        [&](std::promise<butil::Status>& promise_status) {
          try {
            if (!filters.empty()) {
              DoRangeSearch(vector_with_ids.size(), vectors2.get(), radius, range_search_result.get(), filters);
            } else {
              index_id_map2_->range_search(vector_with_ids.size(), vectors2.get(), radius, range_search_result.get());
            }
            promise_status.set_value(butil::Status());
          } catch (std::exception& e) {
            std::string s = fmt::format("VectorIndexFlat::RangeSearch failed. error : {}", e.what());
            promise_status.set_value(butil::Status(pb::error::Errno::EINTERNAL, s));
          }
        },
        std::ref(promise_status));

    butil::Status status2 = future_status.get();

    t.join();

    if (!status2.ok()) {
      DINGO_LOG(ERROR) << status2.error_cstr();
      return status2;
    }
  }

  VectorIndexUtils::FillRangeSearchResult(range_search_result, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

void VectorIndexFlat::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexFlat::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexFlat::SupportSave() { return true; }

butil::Status VectorIndexFlat::Save(const std::string& path) {
  // Warning : read me first !!!!
  // Currently, the save function is executed in the fork child process.
  // When calling glog,
  // the child process will hang.
  // Remove glog temporarily.
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

pb::common::MetricType VectorIndexFlat::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexFlat::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  count = index_id_map2_->id_map.size();
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::GetMemorySize(int64_t& memory_size) {
  RWLockReadGuard guard(&rw_lock_);
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
  RWLockReadGuard guard(&rw_lock_);

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

void VectorIndexFlat::DoRangeSearch(faiss::idx_t n, const faiss::Index::component_t* x, faiss::Index::distance_t radius,
                                    faiss::RangeSearchResult* result,
                                    std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) {
  faiss::SearchParameters flat_search_parameters;
  auto flat_filter = std::make_shared<FlatIDSelector>(filters);
  auto flat_filter_wrapper = std::make_shared<faiss::IDSelectorTranslated>(index_id_map2_->id_map, flat_filter.get());
  flat_search_parameters.sel = flat_filter_wrapper.get();

  index_id_map2_->index->range_search(n, x, radius, result, &flat_search_parameters);

#pragma omp parallel for
  for (faiss::idx_t i = 0; i < result->lims[result->nq]; i++) {
    result->labels[i] = result->labels[i] < 0 ? result->labels[i] : index_id_map2_->id_map[result->labels[i]];
  }
}

}  // namespace dingodb
