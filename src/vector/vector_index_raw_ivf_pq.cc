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

#include "vector/vector_index_raw_ivf_pq.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
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

DEFINE_int64(ivf_pq_need_save_count, 10000, "ivf pq need save count");

VectorIndexRawIvfPq::VectorIndexRawIvfPq(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                         const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                         ThreadPoolPtr thread_pool)
    : VectorIndex(id, vector_index_parameter, epoch, range, thread_pool) {
  bthread_mutex_init(&mutex_, nullptr);

  metric_type_ = vector_index_parameter.ivf_pq_parameter().metric_type();
  dimension_ = vector_index_parameter.ivf_pq_parameter().dimension();

  nlist_ = vector_index_parameter.ivf_pq_parameter().ncentroids();

  nsubvector_ = vector_index_parameter.ivf_pq_parameter().nsubvector();

  nbits_per_idx_ = vector_index_parameter.ivf_pq_parameter().nbits_per_idx();

  nbits_per_idx_ %= 64;

  if (0 == nlist_) {
    nlist_ = Constant::kCreateIvfPqParamNcentroids;
  }

  if (0 == nsubvector_) {
    nsubvector_ = Constant::kCreateIvfPqParamNsubvector;
  }

  if (0 == nbits_per_idx_) {
    nbits_per_idx_ = Constant::kCreateIvfPqParamNbitsPerIdx;
  }

  normalize_ = false;

  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  train_data_size_ = 0;
  // Delay object creation.
}

VectorIndexRawIvfPq::~VectorIndexRawIvfPq() { bthread_mutex_destroy(&mutex_); }

butil::Status VectorIndexRawIvfPq::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                               bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status::OK();
  }

  const auto& [ids, status_ids] = VectorIndexUtils::CheckAndCopyVectorId(vector_with_ids, dimension_);
  if (!status_ids.ok()) {
    DINGO_LOG(ERROR) << status_ids.error_cstr();
    return status_ids;
  }

  const auto& [vectors, status] = VectorIndexUtils::CopyVectorData(vector_with_ids, dimension_, normalize_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("ivf pq not train. train first.");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
  }

  if (is_upsert) {
    faiss::IDSelectorArray sel(vector_with_ids.size(), ids.get());
    index_->remove_ids(sel);
  }
  index_->add_with_ids(vector_with_ids.size(), vectors.get(), ids.get());

  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexRawIvfPq::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexRawIvfPq::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
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

butil::Status VectorIndexRawIvfPq::Delete(const std::vector<int64_t>& delete_ids) {
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  const auto& [ids, status] = VectorIndexUtils::CopyVectorId(delete_ids);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  faiss::IDSelectorArray sel(delete_ids.size(), ids.get());

  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf pq not train. train first. ignored");
      DINGO_LOG(WARNING) << s;
      return butil::Status::OK();
    }

    auto remove_count = index_->remove_ids(sel);
    if (0 == remove_count) {
      DINGO_LOG(ERROR) << fmt::format("not found id : {}", id);
      return butil::Status(pb::error::Errno::EVECTOR_INVALID, fmt::format("not found : {}", id));
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                          const std::vector<std::shared_ptr<FilterFunctor>>& filters,
                                          bool /*reconstruct*/, const pb::common::VectorSearchParameter& parameter,
                                          std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  int32_t nprobe = parameter.ivf_pq().nprobe();
  if (BAIDU_UNLIKELY(nprobe <= 0)) {
    DINGO_LOG(WARNING) << fmt::format("pb::common::VectorSearchParameter ivf_pq nprobe : {} <=0. use default", nprobe);
    nprobe = Constant::kSearchIvfPqParamNprobe;
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

  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf pq not train. train first. ignored");
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

    if (!filters.empty()) {
      auto ivf_pq_filter = filters.empty() ? nullptr : std::make_shared<RawIvfPqIDSelector>(filters);
      ivf_search_parameters.sel = ivf_pq_filter.get();
      index_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(),
                     &ivf_search_parameters);
    } else {
      index_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(),
                     &ivf_search_parameters);
    }
  }

  VectorIndexUtils::FillSearchResult(vector_with_ids, topk, distances, labels, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                               float radius,
                                               const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                               bool /*reconstruct*/, const pb::common::VectorSearchParameter& parameter,
                                               std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  int32_t nprobe = parameter.ivf_pq().nprobe();
  if (BAIDU_UNLIKELY(nprobe <= 0)) {
    DINGO_LOG(WARNING) << fmt::format("pb::common::VectorSearchParameter ivf_pq nprobe : {} <=0. use default", nprobe);
    nprobe = Constant::kSearchIvfPqParamNprobe;
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
    BAIDU_SCOPED_LOCK(mutex_);
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("raw ivf pq not train. train first. ignored");
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

    try {
      if (!filters.empty()) {
        auto ivf_pq_filter = filters.empty() ? nullptr : std::make_shared<RawIvfPqIDSelector>(filters);
        ivf_search_parameters.sel = ivf_pq_filter.get();

        index_->range_search(vector_with_ids.size(), vectors2.get(), radius, range_search_result.get(),
                             &ivf_search_parameters);
      } else {
        index_->range_search(vector_with_ids.size(), vectors2.get(), radius, range_search_result.get(),
                             &ivf_search_parameters);
      }
    } catch (std::exception& e) {
      std::string s = fmt::format("VectorIndexIvfPq::RangeSearch failed. error : {}", e.what());
      DINGO_LOG(ERROR) << s;
      butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  VectorIndexUtils ::FillRangeSearchResult(range_search_result, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

void VectorIndexRawIvfPq::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexRawIvfPq::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

bool VectorIndexRawIvfPq::SupportSave() { return true; }

butil::Status VectorIndexRawIvfPq::Save(const std::string& path) {
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

  // The outside has been locked. Remove the locking operation here.s
  try {
    faiss::write_index(index_.get(), path.c_str());
  } catch (std::exception& e) {
    std::string s =
        fmt::format("VectorIndexRawIvfPq::Save faiss::write_index failed. path : {} error : {}", path, e.what());
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status();
}

butil::Status VectorIndexRawIvfPq::Load(const std::string& path) {
  if (BAIDU_UNLIKELY(path.empty())) {
    std::string s = fmt::format("path empty. not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  // The outside has been locked. Remove the locking operation here.
  faiss::Index* internal_raw_index = nullptr;
  try {
    internal_raw_index = faiss::read_index(path.c_str(), 0);

  } catch (std::exception& e) {
    std::string s =
        fmt::format("VectorIndexRawIvfPq::Load faiss::read_index failed. path : {} error : {}", path, e.what());
    delete internal_raw_index;
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  faiss::IndexIVFPQ* internal_index = dynamic_cast<faiss::IndexIVFPQ*>(internal_raw_index);
  if (BAIDU_UNLIKELY(!internal_index)) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }
    std::string s =
        fmt::format("VectorIndexRawIvfPq::Load faiss::read_index failed. Maybe not IndexIVFPq.  path : {} ", path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // avoid mem leak!!!
  std::unique_ptr<faiss::IndexIVFPQ> internal_index_ivf_pq(internal_index);

  // double check
  if (BAIDU_UNLIKELY(internal_index->d != dimension_)) {
    std::string s = fmt::format("VectorIndexRawIvfPq::Load load dimension : {} !=  dimension_ : {}. path : {}",
                                internal_index->d, dimension_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (BAIDU_UNLIKELY(!internal_index->is_trained)) {
    std::string s = fmt::format("VectorIndexRawIvfPq::Load load is not train. path : {}", path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  switch (metric_type_) {
    case pb::common::METRIC_TYPE_NONE:
      [[fallthrough]];
    case pb::common::METRIC_TYPE_L2: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_L2)) {
        std::string s =
            fmt::format("VectorIndexRawIvfPq::Load load from path type : {} != local type : {}. path : {}",
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
            fmt::format("VectorIndexRawIvfPq::Load load from path type : {} != local type : {}. path : {}",
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
      std::string s = fmt::format("VectorIndexRawIvfPq::Load load from path type : {} != local type : {}. path : {}",
                                  static_cast<int>(internal_index->metric_type), static_cast<int>(metric_type_), path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  if (BAIDU_UNLIKELY(internal_index->nlist != nlist_)) {
    std::string s = fmt::format("VectorIndexRawIvfPq::Load load list : {} !=  (nlist_:{}). path : {}",
                                internal_index->nlist, nlist_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (BAIDU_UNLIKELY(internal_index->pq.M != nsubvector_)) {
    std::string s = fmt::format("VectorIndexRawIvfPq::Load load pq.M : {} !=  (nsubvector_:{}). path : {}",
                                internal_index->pq.M, nsubvector_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  if (BAIDU_UNLIKELY(internal_index->pq.nbits != nbits_per_idx_)) {
    std::string s = fmt::format("VectorIndexRawIvfPq::Load load pq.nbits : {} !=  (nbits_per_idx_:{}). path : {}",
                                internal_index->pq.nbits, nbits_per_idx_, path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  quantizer_.reset();
  index_ = std::move(internal_index_ivf_pq);

  train_data_size_ = index_->ntotal;

  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  DINGO_LOG(INFO) << fmt::format("VectorIndexRawIvfPq::Load success. path : {}", path);

  return butil::Status::OK();
}

int32_t VectorIndexRawIvfPq::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexRawIvfPq::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexRawIvfPq::GetCount(int64_t& count) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (DoIsTrained()) {
    count = index_->ntotal;
  } else {
    count = 0;
  }
  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::GetMemorySize(int64_t& memory_size) {
  BAIDU_SCOPED_LOCK(mutex_);

  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    memory_size = 0;
    return butil::Status::OK();
  }

  auto count = index_->ntotal;
  if (count == 0) {
    memory_size = 0;
    return butil::Status::OK();
  }

  memory_size = 0;

  auto capacity = index_->ntotal * index_->code_size + index_->ntotal * sizeof(faiss::idx_t) +
                  index_->nlist * index_->d * sizeof(float);
  auto centroid_table = index_->pq.M * index_->pq.ksub * index_->pq.dsub * sizeof(float);

  memory_size += (capacity + centroid_table);

  if (faiss::METRIC_L2 == index_->metric_type) {
    auto precomputed_table = index_->nlist * index_->pq.M * index_->pq.ksub * sizeof(float);
    memory_size += precomputed_table;
  }

  return butil::Status::OK();
}

bool VectorIndexRawIvfPq::IsExceedsMaxElements() { return false; }

butil::Status VectorIndexRawIvfPq::Train(const std::vector<float>& train_datas) {
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

  BAIDU_SCOPED_LOCK(mutex_);
  if (BAIDU_UNLIKELY(DoIsTrained())) {
    std::string s = fmt::format("already trained . ignore");
    DINGO_LOG(WARNING) << s;
    return butil::Status::OK();
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

  try {
    index_->train(data_size, train_datas_ptr);
  } catch (std::exception& e) {
    Reset();
    std::string s =
        fmt::format("ivf pq train failed data size : {} dimension : {} exception {}", data_size, dimension_, e.what());
    DINGO_LOG(ERROR) << s;
    Reset();
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  // double check
  if (BAIDU_UNLIKELY(!index_->is_trained)) {
    Reset();
    std::string s =
        fmt::format("ivf pq train failed. data size : {} dimension : {}. internal error", data_size, dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  train_data_size_ = data_size;

  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) {
  std::vector<float> train_datas;
  train_datas.reserve(dimension_ * vectors.size());
  for (const auto& vector : vectors) {
    if (BAIDU_UNLIKELY(dimension_ != vector.vector().float_values().size())) {
      std::string s = fmt::format("ivf pq train failed. float_values size : {} unequal dimension : {}. internal error",
                                  vector.vector().float_values().size(), dimension_);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
    train_datas.insert(train_datas.end(), vector.vector().float_values().begin(), vector.vector().float_values().end());
  }

  return VectorIndexRawIvfPq::Train(train_datas);
}

bool VectorIndexRawIvfPq::NeedToRebuild() {
  BAIDU_SCOPED_LOCK(mutex_);

  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("not trained");
    DINGO_LOG(WARNING) << s;
    return false;
  }

  return (index_->ntotal / 2) >= train_data_size_;
}

bool VectorIndexRawIvfPq::IsTrained() {
  BAIDU_SCOPED_LOCK(mutex_);
  return DoIsTrained();
}

bool VectorIndexRawIvfPq::NeedToSave(int64_t last_save_log_behind) {
  BAIDU_SCOPED_LOCK(mutex_);
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

  if (last_save_log_behind > FLAGS_ivf_pq_need_save_count) {
    return true;
  }

  return false;
}

void VectorIndexRawIvfPq::Init() {
  if (pb::common::MetricType::METRIC_TYPE_L2 == metric_type_) {
    quantizer_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFPQ>(quantizer_.get(), dimension_, nlist_, nsubvector_, nbits_per_idx_,
                                                 faiss::METRIC_L2);
  } else if (pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT == metric_type_) {
    quantizer_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFPQ>(quantizer_.get(), dimension_, nlist_, nsubvector_, nbits_per_idx_,
                                                 faiss::MetricType::METRIC_INNER_PRODUCT);
  } else if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
    quantizer_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFPQ>(quantizer_.get(), dimension_, nlist_, nsubvector_, nbits_per_idx_,
                                                 faiss::MetricType::METRIC_INNER_PRODUCT);
  } else {
    DINGO_LOG(WARNING) << fmt::format("ivf pq : not support metric type : {} use L2 default",
                                      static_cast<int>(metric_type_));
    quantizer_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFPQ>(quantizer_.get(), dimension_, nlist_, nsubvector_, nbits_per_idx_,
                                                 faiss::MetricType::METRIC_L2);
  }
}

bool VectorIndexRawIvfPq::DoIsTrained() {
  if ((index_ && !quantizer_ && index_->own_fields) || (quantizer_ && index_ && !index_->own_fields)) {
    return index_->is_trained;
  }
  return false;
}

void VectorIndexRawIvfPq::Reset() {
  quantizer_->reset();
  index_->reset();
}

}  // namespace dingodb
