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
#include "faiss/impl/IDSelector.h"
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
  metric_type_ = vector_index_parameter.ivf_pq_parameter().metric_type();
  dimension_ = vector_index_parameter.ivf_pq_parameter().dimension();

  nlist_ = vector_index_parameter.ivf_pq_parameter().ncentroids() > 0
               ? vector_index_parameter.ivf_pq_parameter().ncentroids()
               : Constant::kCreateIvfPqParamNcentroids;

  nsubvector_ = vector_index_parameter.ivf_pq_parameter().nsubvector() > 0
                    ? vector_index_parameter.ivf_pq_parameter().nsubvector()
                    : Constant::kCreateIvfPqParamNsubvector;

  nbits_per_idx_ = (vector_index_parameter.ivf_pq_parameter().nbits_per_idx() % 64) > 0
                       ? (vector_index_parameter.ivf_pq_parameter().nbits_per_idx() % 64)
                       : Constant::kCreateIvfPqParamNbitsPerIdx;

  normalize_ = (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_);

  train_data_size_ = 0;
  // Delay object creation.
}

VectorIndexRawIvfPq::~VectorIndexRawIvfPq() {}

butil::Status VectorIndexRawIvfPq::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                               bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  const auto& ids = VectorIndexUtils::ExtractVectorId(vector_with_ids);
  const auto& vector_values = VectorIndexUtils::ExtractVectorValue(vector_with_ids, dimension_, normalize_);

  RWLockWriteGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, "ivf pq not train. train first");
  }

  if (is_upsert) {
    faiss::IDSelectorBatch sel(vector_with_ids.size(), ids.get());
    index_->remove_ids(sel);
  }
  index_->add_with_ids(vector_with_ids.size(), vector_values.get(), ids.get());

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
    if (!status.ok()) {
      return status;
    }

    // try again
    status = AddOrUpsert(vector_with_ids, is_upsert);
    if (BAIDU_LIKELY(!status.ok())) {
      return status;
    }
  }

  return status;
}

butil::Status VectorIndexRawIvfPq::Delete(const std::vector<int64_t>& delete_ids) {
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  const auto& ids = VectorIndexUtils::CastVectorId(delete_ids);
  faiss::IDSelectorBatch sel(delete_ids.size(), ids.get());

  {
    RWLockWriteGuard guard(&rw_lock_);

    if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
      return butil::Status::OK();
    }

    auto remove_count = index_->remove_ids(sel);
    if (0 == remove_count) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.raw_ivf_pq][id({})] remove not found vector id.", Id());
      return butil::Status(pb::error::Errno::EVECTOR_INVALID, "remove not found vector id");
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                          const std::vector<std::shared_ptr<FilterFunctor>>& filters,
                                          bool /*reconstruct*/, const pb::common::VectorSearchParameter& parameter,
                                          std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  if (topk <= 0) return butil::Status::OK();
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  int32_t nprobe = parameter.ivf_pq().nprobe() > 0 ? parameter.ivf_pq().nprobe() : Constant::kSearchIvfPqParamNprobe;

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(topk * vector_with_ids.size(), 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk * vector_with_ids.size(), -1);

  const auto& vector_values = VectorIndexUtils::ExtractVectorValue(vector_with_ids, dimension_, normalize_);

  {
    RWLockReadGuard guard(&rw_lock_);

    if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
      for (size_t row = 0; row < vector_with_ids.size(); ++row) {
        auto& result = results.emplace_back();
      }
      return butil::Status::OK();
    }

    faiss::IVFSearchParameters ivf_search_parameters;
    ivf_search_parameters.nprobe = std::min(nprobe, static_cast<int32_t>(index_->nlist));
    ivf_search_parameters.max_codes = 0;
    ivf_search_parameters.quantizer_params = nullptr;  // search for nlist . ignore

    if (!filters.empty()) {
      auto ivf_pq_filter = filters.empty() ? nullptr : std::make_shared<RawIvfPqIDSelector>(filters);
      ivf_search_parameters.sel = ivf_pq_filter.get();
      index_->search(vector_with_ids.size(), vector_values.get(), topk, distances.data(), labels.data(),
                     &ivf_search_parameters);
    } else {
      index_->search(vector_with_ids.size(), vector_values.get(), topk, distances.data(), labels.data(),
                     &ivf_search_parameters);
    }
  }

  VectorIndexUtils::FillSearchResult(vector_with_ids, topk, distances, labels, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.raw_ivf_pq][id({})] result size {}", Id(), results.size());

  return butil::Status::OK();
}

butil::Status VectorIndexRawIvfPq::RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                               float radius,
                                               const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                               bool /*reconstruct*/, const pb::common::VectorSearchParameter& parameter,
                                               std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  int32_t nprobe = parameter.ivf_pq().nprobe() > 0 ? parameter.ivf_pq().nprobe() : Constant::kSearchIvfPqParamNprobe;

  const auto& vectors_values = VectorIndexUtils::ExtractVectorValue(vector_with_ids, dimension_, normalize_);

  std::unique_ptr<faiss::RangeSearchResult> range_search_result =
      std::make_unique<faiss::RangeSearchResult>(vector_with_ids.size());

  if (metric_type_ == pb::common::MetricType::METRIC_TYPE_COSINE ||
      metric_type_ == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
    radius = 1.0F - radius;
  }

  {
    RWLockReadGuard guard(&rw_lock_);

    if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
      DINGO_LOG(WARNING) << fmt::format("raw ivf pq not train. train first. ignored");

      for (size_t row = 0; row < vector_with_ids.size(); ++row) {
        auto& result = results.emplace_back();
      }

      return butil::Status::OK();
    }

    faiss::IVFSearchParameters ivf_search_parameters;
    ivf_search_parameters.nprobe = std::min(nprobe, static_cast<int32_t>(index_->nlist));
    ivf_search_parameters.max_codes = 0;
    ivf_search_parameters.quantizer_params = nullptr;  // search for nlist . ignore

    try {
      if (!filters.empty()) {
        auto ivf_pq_filter = filters.empty() ? nullptr : std::make_shared<RawIvfPqIDSelector>(filters);
        ivf_search_parameters.sel = ivf_pq_filter.get();

        index_->range_search(vector_with_ids.size(), vectors_values.get(), radius, range_search_result.get(),
                             &ivf_search_parameters);
      } else {
        index_->range_search(vector_with_ids.size(), vectors_values.get(), radius, range_search_result.get(),
                             &ivf_search_parameters);
      }
    } catch (std::exception& e) {
      std::string s = fmt::format("range search exception: {}", e.what());
      DINGO_LOG(ERROR) << fmt::format("[vector_index.raw_ivf_pq][id({})] {}", Id(), s);
      butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  VectorIndexUtils ::FillRangeSearchResult(range_search_result, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.raw_ivf_pq][id({})] result size {}", Id(), results.size());

  return butil::Status::OK();
}

void VectorIndexRawIvfPq::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexRawIvfPq::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexRawIvfPq::SupportSave() { return true; }

butil::Status VectorIndexRawIvfPq::Save(const std::string& path) {
  // Warning : read me first !!!!
  // Currently, the save function is executed in the fork child process.
  // When calling glog,
  // the child process will hang.
  // Remove glog temporarily.
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  // The outside has been locked. Remove the locking operation here.s
  try {
    faiss::write_index(index_.get(), path.c_str());
  } catch (std::exception& e) {
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("write index exception: {}", e.what()));
  }

  return butil::Status();
}

butil::Status VectorIndexRawIvfPq::Load(const std::string& path) {
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  // The outside has been locked. Remove the locking operation here.
  faiss::Index* internal_raw_index = nullptr;
  try {
    internal_raw_index = faiss::read_index(path.c_str(), 0);

  } catch (std::exception& e) {
    delete internal_raw_index;
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("read index exception: {} {}", path, e.what()));
  }

  faiss::IndexIVFPQ* internal_index = dynamic_cast<faiss::IndexIVFPQ*>(internal_raw_index);
  if (BAIDU_UNLIKELY(!internal_index)) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }

    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("type cast failed"));
  }

  // avoid mem leak!!!
  std::unique_ptr<faiss::IndexIVFPQ> internal_index_ivf_pq(internal_index);

  // double check
  if (BAIDU_UNLIKELY(internal_index->d != dimension_)) {
    return butil::Status(pb::error::Errno::EINTERNAL,
                         fmt::format("dimension not match, {} {}", internal_index->d, dimension_));
  }

  if (BAIDU_UNLIKELY(!internal_index->is_trained)) {
    return butil::Status(pb::error::Errno::EINTERNAL, "already trained");
  }

  switch (metric_type_) {
    case pb::common::METRIC_TYPE_NONE:
      [[fallthrough]];
    case pb::common::METRIC_TYPE_L2: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_L2)) {
        std::string s = fmt::format("metric type not match, {} {}", static_cast<int>(internal_index->metric_type),
                                    pb::common::MetricType_Name(metric_type_));
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }

    case pb::common::METRIC_TYPE_INNER_PRODUCT:
    case pb::common::METRIC_TYPE_COSINE: {
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_INNER_PRODUCT)) {
        std::string s = fmt::format("metric type not match, {} {}", static_cast<int>(internal_index->metric_type),
                                    pb::common::MetricType_Name(metric_type_));
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }
    case pb::common::MetricType_INT_MIN_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    case pb::common::MetricType_INT_MAX_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("metric type not match, {} {}", static_cast<int>(internal_index->metric_type),
                                  pb::common::MetricType_Name(metric_type_));
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  if (BAIDU_UNLIKELY(internal_index->nlist != nlist_)) {
    return butil::Status(pb::error::Errno::EINTERNAL,
                         fmt::format("nlist not match: {} {}", internal_index->nlist, nlist_));
  }

  if (BAIDU_UNLIKELY(internal_index->pq.M != nsubvector_)) {
    return butil::Status(pb::error::Errno::EINTERNAL,
                         fmt::format("M not match: {} {}", internal_index->pq.M, nsubvector_));
  }

  if (BAIDU_UNLIKELY(internal_index->pq.nbits != nbits_per_idx_)) {
    return butil::Status(pb::error::Errno::EINTERNAL,
                         fmt::format("nbits not match: {} {}", internal_index->pq.nbits, nbits_per_idx_));
  }

  quantizer_.reset();
  index_ = std::move(internal_index_ivf_pq);

  train_data_size_ = index_->ntotal;

  if (pb::common::MetricType::METRIC_TYPE_COSINE == metric_type_) {
    normalize_ = true;
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.raw_ivf_pq][id({})] load finsh, path: {}", Id(), path);

  return butil::Status::OK();
}

int32_t VectorIndexRawIvfPq::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexRawIvfPq::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexRawIvfPq::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);

  if (IsTrainedImpl()) {
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
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
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

butil::Status VectorIndexRawIvfPq::Train(std::vector<float>& train_datas) {
  size_t data_size = train_datas.size() / dimension_;
  if (data_size == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "data size invalid");
  }
  if (train_datas.size() % dimension_ != 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("dimension not match {} {}", train_datas.size(), dimension_));
  }

  RWLockWriteGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(IsTrainedImpl())) {
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.raw_ivf_pq][id({})] train size: {}", Id(), train_datas.size());

  // init index
  Init();

  train_data_size_ = 0;

  if (normalize_) {
    for (size_t i = 0; i < data_size; i++) {
      VectorIndexUtils::NormalizeVectorForFaiss(train_datas.data() + i * dimension_, dimension_);
    }
  }

  try {
    index_->train(data_size, train_datas.data());
    if (!index_->is_trained) {
      Reset();
      return butil::Status(pb::error::Errno::EINTERNAL, "check raw_ivf_pq index train failed");
    }
  } catch (std::exception& e) {
    Reset();
    return butil::Status(pb::error::Errno::EINTERNAL, "train raw_ivf_pq exception");
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
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return false;
  }

  return (index_->ntotal / 2) >= train_data_size_;
}

bool VectorIndexRawIvfPq::IsTrained() {
  RWLockReadGuard guard(&rw_lock_);
  return IsTrainedImpl();
}

bool VectorIndexRawIvfPq::NeedToSave(int64_t last_save_log_behind) {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return false;
  }

  if (index_->ntotal == 0) {
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
    DINGO_LOG(ERROR) << fmt::format("[vector_index.raw_ivf_pq][id({})] unknown index type.", Id());
    quantizer_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
    index_ = std::make_unique<faiss::IndexIVFPQ>(quantizer_.get(), dimension_, nlist_, nsubvector_, nbits_per_idx_,
                                                 faiss::MetricType::METRIC_L2);
  }
}

bool VectorIndexRawIvfPq::IsTrainedImpl() {
  bool is_trained = ((index_ && !quantizer_ && index_->own_fields) || (quantizer_ && index_ && !index_->own_fields))
                        ? index_->is_trained
                        : false;

  return is_trained;
}

void VectorIndexRawIvfPq::Reset() {
  quantizer_->reset();
  index_->reset();
}

}  // namespace dingodb
