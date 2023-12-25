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

#include "vector/vector_index_ivf_flat.h"

#include <algorithm>
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
#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
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
DEFINE_int64(ivf_flat_need_save_count, 10000, "ivf flat need save count");

VectorIndexIvfFlat::VectorIndexIvfFlat(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                       const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, epoch, range) {
  metric_type_ = vector_index_parameter.ivf_flat_parameter().metric_type();
  dimension_ = vector_index_parameter.ivf_flat_parameter().dimension();

  nlist_org_ = vector_index_parameter.ivf_flat_parameter().ncentroids();

  if (0 == nlist_org_) {
    nlist_org_ = Constant::kCreateIvfFlatParamNcentroids;
  }

  nlist_ = nlist_org_;

  normalize_ = false;
  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  train_data_size_ = 0;
  // Delay object creation.
}

VectorIndexIvfFlat::~VectorIndexIvfFlat() = default;

butil::Status VectorIndexIvfFlat::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
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
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("ivf flat not train. train first.");
    DINGO_LOG(WARNING) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
  }

  std::thread([&]() {
    if (is_upsert) {
      faiss::IDSelectorArray sel(vector_with_ids.size(), ids2.get());
      index_->remove_ids(sel);
    }
    index_->add_with_ids(vector_with_ids.size(), vectors2.get(), ids2.get());
  }).join();

  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                     bool is_upsert) {
  auto status = AddOrUpsert(vector_with_ids, is_upsert);
  if (BAIDU_UNLIKELY(pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code())) {
    status = Train(vector_with_ids);
    if (BAIDU_LIKELY(status.ok())) {
      // try again
      status = AddOrUpsert(vector_with_ids, is_upsert);
      if (BAIDU_LIKELY(!status.ok())) {  // AddOrUpsert
        DINGO_LOG(ERROR) << status;
        return status;
      }
    } else {  // Train failed
      DINGO_LOG(ERROR) << status;
      return status;
    }
  }

  return status;
}

butil::Status VectorIndexIvfFlat::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexIvfFlat::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexIvfFlat::Delete(const std::vector<int64_t>& delete_ids) {
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
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf flat not train. train first. ignored");
      DINGO_LOG(WARNING) << s;
      return butil::Status::OK();
    }

    std::thread([&]() { remove_count = index_->remove_ids(sel); }).join();
  }

  if (0 == remove_count) {
    DINGO_LOG(ERROR) << fmt::format("not found id : {}", id);
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, fmt::format("not found : {}", id));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                         std::vector<std::shared_ptr<FilterFunctor>> filters, bool,
                                         const pb::common::VectorSearchParameter& parameter,
                                         std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  int32_t nprobe = parameter.ivf_flat().nprobe();
  if (BAIDU_UNLIKELY(nprobe <= 0)) {
    DINGO_LOG(WARNING) << fmt::format("pb::common::VectorSearchParameter ivf_flat nprobe : {} <=0. use default",
                                      nprobe);
    nprobe = Constant::kSearchIvfFlatParamNprobe;
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

  {
    RWLockReadGuard guard(&rw_lock_);
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf flat not train. train first. ignored");
      DINGO_LOG(WARNING) << s;

      for (size_t row = 0; row < vector_with_ids.size(); ++row) {
        auto& result = results.emplace_back();
      }

      return butil::Status::OK();
    }

    if (BAIDU_UNLIKELY(nprobe <= 0)) {
      nprobe = index_->nprobe;
    }

    // Prevent users from passing parameters out of bounds.
    nprobe = std::min(nprobe, static_cast<int32_t>(index_->nlist));

    faiss::IVFSearchParameters ivf_search_parameters;
    ivf_search_parameters.nprobe = nprobe;
    ivf_search_parameters.max_codes = 0;
    ivf_search_parameters.quantizer_params = nullptr;  // search for nlist . ignore

    // use std::thread to call faiss functions
    std::thread t([&]() {
      if (!filters.empty()) {
        auto ivf_flat_filter = filters.empty() ? nullptr : std::make_shared<IvfFlatIDSelector>(filters);
        ivf_search_parameters.sel = ivf_flat_filter.get();
        index_->search(vector_with_ids.size(), vectors2.get(), topk, distances.data(), labels.data(),
                       &ivf_search_parameters);
      } else {
        index_->search(vector_with_ids.size(), vectors2.get(), topk, distances.data(), labels.data(),
                       &ivf_search_parameters);
      }
    });
    t.join();
  }

  VectorIndexUtils::FillSearchResult(vector_with_ids, topk, distances, labels, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::RangeSearch(std::vector<pb::common::VectorWithId> vector_with_ids, float radius,
                                              std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                              bool /*reconstruct*/, const pb::common::VectorSearchParameter& parameter,
                                              std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  int32_t nprobe = parameter.ivf_flat().nprobe();
  if (BAIDU_UNLIKELY(nprobe <= 0)) {
    DINGO_LOG(WARNING) << fmt::format("pb::common::VectorSearchParameter ivf_flat nprobe : {} <=0. use default",
                                      nprobe);
    nprobe = Constant::kSearchIvfFlatParamNprobe;
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
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf flat not train. train first. ignored");
      DINGO_LOG(WARNING) << s;

      for (size_t row = 0; row < vector_with_ids.size(); ++row) {
        auto& result = results.emplace_back();
      }

      return butil::Status::OK();
    }

    if (BAIDU_UNLIKELY(nprobe <= 0)) {
      nprobe = index_->nprobe;
    }

    // Prevent users from passing parameters out of bounds.
    nprobe = std::min(nprobe, static_cast<int32_t>(index_->nlist));

    faiss::IVFSearchParameters ivf_search_parameters;
    ivf_search_parameters.nprobe = nprobe;
    ivf_search_parameters.max_codes = 0;
    ivf_search_parameters.quantizer_params = nullptr;  // search for nlist . ignore

    std::promise<butil::Status> promise_status;
    std::future<butil::Status> future_status = promise_status.get_future();
    // use std::thread to call faiss functions
    std::thread t(
        [&](std::promise<butil::Status>& promise_status) {
          try {
            if (!filters.empty()) {
              auto ivf_flat_filter = filters.empty() ? nullptr : std::make_shared<IvfFlatIDSelector>(filters);
              ivf_search_parameters.sel = ivf_flat_filter.get();
              index_->range_search(vector_with_ids.size(), vectors2.get(), radius, range_search_result.get(),
                                   &ivf_search_parameters);
            } else {
              index_->range_search(vector_with_ids.size(), vectors2.get(), radius, range_search_result.get(),
                                   &ivf_search_parameters);
            }
            promise_status.set_value(butil::Status());
          } catch (std::exception& e) {
            std::string s = fmt::format("VectorIndexIvfFlat::RangeSearch failed. error : {}", e.what());
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

  VectorIndexUtils ::FillRangeSearchResult(range_search_result, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

void VectorIndexIvfFlat::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexIvfFlat::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexIvfFlat::SupportSave() { return true; }

butil::Status VectorIndexIvfFlat::Save(const std::string& path) {
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
          faiss::write_index(index_.get(), path.c_str());
          promise_status.set_value(butil::Status());
        } catch (std::exception& e) {
          std::string s =
              fmt::format("VectorIndexIvfFlat::Save faiss::write_index failed. path : {} error : {}", path, e.what());
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
    // DINGO_LOG(INFO) << fmt::format("VectorIndexIvfFlat::Save success. path : {}", path);
  }

  return status;
}

butil::Status VectorIndexIvfFlat::Load(const std::string& path) {
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
          internal_raw_index = faiss::read_index(path.c_str(), 0);
          promise_status.set_value(std::pair<faiss::Index*, butil::Status>(internal_raw_index, butil::Status()));
        } catch (std::exception& e) {
          std::string s =
              fmt::format("VectorIndexIvfFlat::Load faiss::read_index failed. path : {} error : {}", path, e.what());
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

  faiss::IndexIVFFlat* internal_index = dynamic_cast<faiss::IndexIVFFlat*>(internal_raw_index);
  if (BAIDU_UNLIKELY(!internal_index)) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }
    std::string s =
        fmt::format("VectorIndexIvfFlat::Load faiss::read_index failed. Maybe not IndexIVFFlat.  path : {} ", path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // avoid mem leak!!!
  std::unique_ptr<faiss::IndexIVFFlat> internal_index_ivf_flat(internal_index);

  // double check
  if (BAIDU_UNLIKELY(internal_index->d != dimension_)) {
    std::string s = fmt::format("VectorIndexIvfFlat::Load load dimension : {} !=  dimension_ : {}. path : {}",
                                internal_index->d, dimension_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (BAIDU_UNLIKELY(!internal_index->is_trained)) {
    std::string s = fmt::format("VectorIndexIvfFlat::Load load is not train. path : {}", path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  switch (metric_type_) {
    case pb::common::METRIC_TYPE_NONE:
      [[fallthrough]];
    case pb::common::METRIC_TYPE_L2: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_L2)) {
        std::string s =
            fmt::format("VectorIndexIvfFlat::Load load from path type : {} != local type : {}. path : {}",
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
            fmt::format("VectorIndexIvfFlat::Load load from path type : {} != local type : {}. path : {}",
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
    default:
      break;
  }

  if (BAIDU_UNLIKELY(internal_index->nlist != nlist_ && internal_index->nlist != nlist_org_)) {
    std::string s = fmt::format("VectorIndexIvfFlat::Load load list : {} !=  (nlist_:{} or nlist_org_ : {}). path : {}",
                                internal_index->nlist, nlist_, nlist_org_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  quantizer_.reset();
  index_ = std::move(internal_index_ivf_flat);

  nlist_ = index_->nlist;
  train_data_size_ = index_->ntotal;

  // this is important.
  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  DINGO_LOG(INFO) << fmt::format("VectorIndexIvfFlat::Load success. path : {}", path);

  return butil::Status::OK();
}

int32_t VectorIndexIvfFlat::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexIvfFlat::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexIvfFlat::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (DoIsTrained()) {
    count = index_->ntotal;
  } else {
    count = 0;
  }
  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::GetMemorySize(int64_t& memory_size) {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    memory_size = 0;
    return butil::Status::OK();
  }

  auto count = index_->ntotal;
  if (count == 0) {
    memory_size = 0;
    return butil::Status::OK();
  }

  memory_size = count * sizeof(faiss::idx_t) + count * dimension_ * sizeof(faiss::Index::component_t) +
                nlist_ * dimension_ * sizeof(faiss::Index::component_t);
  return butil::Status::OK();
}

bool VectorIndexIvfFlat::IsExceedsMaxElements() { return false; }

butil::Status VectorIndexIvfFlat::Train(const std::vector<float>& train_datas) {
  size_t data_size = train_datas.size() / dimension_;

  // check
  if (BAIDU_UNLIKELY(0 != train_datas.size() % dimension_)) {
    std::string s =
        fmt::format("train_datas float size : {} , dimension : {}, Not divisible ", train_datas.size(), dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (BAIDU_UNLIKELY(0 == data_size)) {
    std::string s = fmt::format("train_datas zero not support ");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  faiss::ClusteringParameters clustering_parameters;
  if (BAIDU_UNLIKELY(data_size < (clustering_parameters.min_points_per_centroid * nlist_))) {
    std::string s = fmt::format("train_datas size : {} not enough. suggest at least : {}.  ignore", data_size,
                                clustering_parameters.min_points_per_centroid * nlist_);
    DINGO_LOG(WARNING) << s;
  } else if (BAIDU_UNLIKELY(data_size >= clustering_parameters.min_points_per_centroid * nlist_ &&
                            data_size < clustering_parameters.max_points_per_centroid * nlist_)) {
    std::string s = fmt::format("train_datas size : {} not enough. suggest at least : {}.  ignore", data_size,
                                clustering_parameters.max_points_per_centroid * nlist_);
    DINGO_LOG(WARNING) << s;
  }

  RWLockWriteGuard guard(&rw_lock_);
  if (BAIDU_UNLIKELY(DoIsTrained())) {
    std::string s = fmt::format("already trained . ignore");
    DINGO_LOG(WARNING) << s;
    return butil::Status::OK();
  }

  // critical code
  if (BAIDU_UNLIKELY(data_size < nlist_)) {
    std::string s = fmt::format("train_datas size : {} too small. nlist : {} degenerate to 1", data_size, nlist_);
    DINGO_LOG(WARNING) << s;
    nlist_ = 1;
  }

  // init index
  Init();

  train_data_size_ = 0;

  float* train_datas_ptr = const_cast<float*>(train_datas.data());
  std::vector<float> train_datas_for_normalize;
  if (normalize_) {
    try {
      train_datas_for_normalize = train_datas;
    } catch (const std::exception& e) {
      Reset();
      std::string s = fmt::format("copy train data exception : {}", e.what());
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    for (size_t i = 0; i < data_size; i++) {
      VectorIndexUtils::NormalizeVectorForFaiss(const_cast<float*>(train_datas_for_normalize.data()) + i * dimension_,
                                                dimension_);
    }

    train_datas_ptr = const_cast<float*>(train_datas_for_normalize.data());
  }

  std::promise<butil::Status> promise_status;
  std::future<butil::Status> future_status = promise_status.get_future();
  std::thread t(
      [&](std::promise<butil::Status>& promise_status) {
        try {
          index_->train(data_size, train_datas_ptr);
          promise_status.set_value(butil::Status());
        } catch (std::exception& e) {
          Reset();
          std::string s = fmt::format("ivf flat train failed data size : {} dimension : {} exception {}", data_size,
                                      dimension_, e.what());
          LOG(ERROR) << "["
                     << "lambda ivf_flat::train"
                     << "] " << s;
          promise_status.set_value(butil::Status(pb::error::Errno::EINTERNAL, s));
        }
      },
      std::ref(promise_status));

  butil::Status status = future_status.get();
  t.join();

  if (!status.ok()) {
    Reset();
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  // double check
  if (BAIDU_UNLIKELY(!index_->is_trained)) {
    Reset();
    std::string s =
        fmt::format("ivf flat train failed. data size : {} dimension : {}. internal error", data_size, dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  train_data_size_ = data_size;

  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) {
  std::vector<float> train_datas;
  train_datas.reserve(dimension_ * vectors.size());
  for (const auto& vector : vectors) {
    if (BAIDU_UNLIKELY(dimension_ != vector.vector().float_values().size())) {
      std::string s =
          fmt::format("ivf flat train failed. float_values size : {} unequal dimension : {}. internal error",
                      vector.vector().float_values().size(), dimension_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
    train_datas.insert(train_datas.end(), vector.vector().float_values().begin(), vector.vector().float_values().end());
  }

  return VectorIndexIvfFlat::Train(train_datas);
}

bool VectorIndexIvfFlat::NeedToRebuild() {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("not trained");
    DINGO_LOG(WARNING) << s;
    return false;
  }

  faiss::ClusteringParameters clustering_parameters;

  // nlist always = 1 not train , flat actually.
  if (BAIDU_UNLIKELY(nlist_ == nlist_org_ && 1 == nlist_)) {
    return false;
  }

  if (BAIDU_UNLIKELY(nlist_ != nlist_org_ && 1 == nlist_ &&
                     index_->ntotal >= clustering_parameters.max_points_per_centroid * nlist_org_)) {
    return true;
  }

  if (BAIDU_UNLIKELY(nlist_ == nlist_org_ && 1 != nlist_ &&
                     index_->ntotal >= clustering_parameters.max_points_per_centroid * nlist_org_)) {
    return train_data_size_ <= (index_->ntotal / 2);
  }

  return false;
}

bool VectorIndexIvfFlat::IsTrained() {
  RWLockReadGuard guard(&rw_lock_);
  return DoIsTrained();
}

bool VectorIndexIvfFlat::NeedToSave(int64_t last_save_log_behind) {
  RWLockReadGuard guard(&rw_lock_);
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("not trained. train first");
    DINGO_LOG(WARNING) << s;
    return false;
  }

  int64_t element_count = 0;

  element_count = index_->ntotal;

  if (element_count == 0) {
    return false;
  }

  if (last_save_log_behind > FLAGS_ivf_flat_need_save_count) {
    return true;
  }

  return false;
}

void VectorIndexIvfFlat::Init() {
  if (pb::common::MetricType::METRIC_TYPE_L2 == metric_type_) {
    quantizer_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFFlat>(quantizer_.get(), dimension_, nlist_);
  } else if (pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT == metric_type_) {
    quantizer_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFFlat>(quantizer_.get(), dimension_, nlist_,
                                                   faiss::MetricType::METRIC_INNER_PRODUCT);
  } else if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
    quantizer_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFFlat>(quantizer_.get(), dimension_, nlist_,
                                                   faiss::MetricType::METRIC_INNER_PRODUCT);
  } else {
    DINGO_LOG(WARNING) << fmt::format("Ivf Flat : not support metric type : {} use L2 default",
                                      static_cast<int>(metric_type_));
    quantizer_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFFlat>(quantizer_.get(), dimension_, nlist_, faiss::MetricType::METRIC_L2);
  }
}

bool VectorIndexIvfFlat::DoIsTrained() {
  if ((index_ && !quantizer_ && index_->own_fields) || (quantizer_ && index_ && !index_->own_fields)) {
    return index_->is_trained;
  }
  return false;
}

void VectorIndexIvfFlat::Reset() {
  quantizer_->reset();
  index_->reset();
  nlist_ = nlist_org_;
}

}  // namespace dingodb
