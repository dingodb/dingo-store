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

VectorIndexIvfFlat::VectorIndexIvfFlat(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                       const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, range) {
  bthread_mutex_init(&mutex_, nullptr);

  metric_type_ = vector_index_parameter.ivf_flat_parameter().metric_type();
  dimension_ = vector_index_parameter.ivf_flat_parameter().dimension();

  nlist_org_ = vector_index_parameter.ivf_flat_parameter().ncentroids();

  if (0 == nlist_org_) {
    nlist_org_ = Constant::kCreateIvfFlatParamNcentroids;
  }

  nlist_ = nlist_org_;

  normalize_ = false;

  train_data_size_ = 0;
  // Delay object creation.
}

VectorIndexIvfFlat::~VectorIndexIvfFlat() { bthread_mutex_destroy(&mutex_); }

butil::Status VectorIndexIvfFlat::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                              bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status::OK();
  }

  // check
  {
    size_t i = 0;
    for (const auto& vector_with_id : vector_with_ids) {
      uint32_t input_dimension = vector_with_id.vector().float_values_size();
      if (input_dimension != static_cast<size_t>(dimension_)) {
        std::string s = fmt::format("Ivf Flat id.no : {}: float size : {} not equal to  dimension(create) : {}", i,
                                    input_dimension, dimension_);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
      }
      i++;
    }
  }

  std::unique_ptr<faiss::idx_t[]> ids;
  try {
    ids = std::make_unique<faiss::idx_t[]>(vector_with_ids.size());  // do not modify reset method. this fast and safe.
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for ids: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, "Failed to allocate memory for ids, error: %s", e.what());
  }

  for (size_t i = 0; i < vector_with_ids.size(); ++i) {
    ids[i] = static_cast<faiss::idx_t>(vector_with_ids[i].id());
  }

  BAIDU_SCOPED_LOCK(mutex_);
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("ivf flat not train. train first.");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
  }

  if (is_upsert) {
    faiss::IDSelectorArray sel(vector_with_ids.size(), ids.get());
    index_->remove_ids(sel);
  }

  std::unique_ptr<float[]> vectors;
  try {
    vectors = std::make_unique<float[]>(vector_with_ids.size() *
                                        dimension_);  // do not modify reset method. this fast and safe.
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

butil::Status VectorIndexIvfFlat::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsert(vector_with_ids, true);
}

butil::Status VectorIndexIvfFlat::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsert(vector_with_ids, false);
}

butil::Status VectorIndexIvfFlat::Delete(const std::vector<uint64_t>& delete_ids) {
  if (delete_ids.empty()) {
    DINGO_LOG(WARNING) << "delete ids is empty";
    return butil::Status::OK();
  }

  std::unique_ptr<faiss::idx_t[]> ids;
  try {
    ids = std::make_unique<faiss::idx_t[]>(delete_ids.size());
  } catch (std::bad_alloc& e) {
    std::string s = fmt::format("Failed to allocate memory for ids: {}", e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, "Failed to allocate memory for ids, error: %s", e.what());
  }

  for (size_t i = 0; i < delete_ids.size(); ++i) {
    ids[i] = static_cast<faiss::idx_t>(delete_ids[i]);
  }

  faiss::IDSelectorArray sel(delete_ids.size(), ids.get());

  size_t remove_count = 0;
  {
    BAIDU_SCOPED_LOCK(mutex_);
    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf flat not train. train first.");
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
    }

    remove_count = index_->remove_ids(sel);
  }

  if (0 == remove_count) {
    DINGO_LOG(ERROR) << fmt::format("not found id : {}", id);
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, fmt::format("not found : {}", id));
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                         std::vector<std::shared_ptr<FilterFunctor>> filters,
                                         std::vector<pb::index::VectorWithDistanceResult>& results,
                                         bool /*reconstruct*/,
                                         const pb::common::VectorSearchParameter& parameter) {  // NOLINT
  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  if (!DoIsTrained()) {
    DINGO_LOG(WARNING) << "ivf flat not train. train first. vector_index_id: " << Id();
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, "ivf flat not train. train first.");
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

  std::unique_ptr<float[]> vectors;
  try {
    vectors = std::make_unique<float[]>(vector_with_ids.size() * dimension_);
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
    BAIDU_SCOPED_LOCK(mutex_);
    if (BAIDU_UNLIKELY(nprobe <= 0)) {
      nprobe = index_->nprobe;
    }

    // Prevent users from passing parameters out of bounds.
    nprobe = std::min(nprobe, static_cast<int32_t>(index_->nlist));

    if (BAIDU_UNLIKELY(!DoIsTrained())) {
      std::string s = fmt::format("ivf flat not train. train first.");
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
    }

    faiss::IVFSearchParameters ivf_search_parameters;
    ivf_search_parameters.nprobe = nprobe;
    ivf_search_parameters.max_codes = 0;
    ivf_search_parameters.quantizer_params = nullptr;  // search for nlist . ignore

    // use std::thread to call faiss functions
    std::thread t([&]() {
      if (!filters.empty()) {
        auto ivf_flat_filter = filters.empty() ? nullptr : std::make_shared<IvfFlatIDSelector>(filters);
        ivf_search_parameters.sel = ivf_flat_filter.get();
        index_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(),
                       &ivf_search_parameters);
      } else {
        index_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data(),
                       &ivf_search_parameters);
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

void VectorIndexIvfFlat::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexIvfFlat::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

bool VectorIndexIvfFlat::SupportSave() { return true; }

butil::Status VectorIndexIvfFlat::Save(const std::string& path) {
  if (BAIDU_UNLIKELY(path.empty())) {
    std::string s = fmt::format("path empty. not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  BAIDU_SCOPED_LOCK(mutex_);
  try {
    faiss::write_index(index_.get(), path.c_str());
  } catch (std::exception& e) {
    std::string s =
        fmt::format("VectorIndexIvfFlat::Save faiss::write_index failed. path : {} error : {}", path, e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  DINGO_LOG(INFO) << fmt::format("VectorIndexIvfFlat::Save success. path : {}", path);

  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::Load(const std::string& path) {
  if (BAIDU_UNLIKELY(path.empty())) {
    std::string s = fmt::format("path empty. not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, s);
  }

  BAIDU_SCOPED_LOCK(mutex_);
  try {
    faiss::IndexIVFFlat* internal_index = dynamic_cast<faiss::IndexIVFFlat*>(faiss::read_index(path.c_str(), 0));
    if (BAIDU_UNLIKELY(!internal_index)) {
      std::string s =
          fmt::format("VectorIndexIvfFlat::Load faiss::read_index failed. Maybe not IndexIVFFlat.  path : {} ", path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

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
      std::string s =
          fmt::format("VectorIndexIvfFlat::Load load list : {} !=  (nlist_:{} or nlist_org_ : {}). path : {}",
                      internal_index->nlist, nlist_, nlist_org_, path);
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }

    quantizer_.reset();
    index_.reset(internal_index);

    nlist_ = index_->nlist;
    train_data_size_ = index_->ntotal;

    DINGO_LOG(INFO) << fmt::format("VectorIndexIvfFlat::Load success. path : {}", path);
  } catch (std::exception& e) {
    std::string s =
        fmt::format("VectorIndexIvfFlat::Load faiss::read_index failed. path : {} error : {}", path, e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
  }

  return butil::Status::OK();
}

int32_t VectorIndexIvfFlat::GetDimension() { return this->dimension_; }

butil::Status VectorIndexIvfFlat::GetCount(uint64_t& count) {
  BAIDU_SCOPED_LOCK(mutex_);
  if (DoIsTrained()) {
    count = index_->ntotal;
  } else {
    count = 0;
  }
  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::GetDeletedCount(uint64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexIvfFlat::GetMemorySize(uint64_t& memory_size) {
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

  BAIDU_SCOPED_LOCK(mutex_);
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

  try {
    index_->train(data_size, train_datas.data());
  } catch (std::exception& e) {
    Reset();
    std::string s = fmt::format("ivf flat train failed data size : {} dimension : {} exception {}", data_size,
                                dimension_, e.what());
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EINTERNAL, s);
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
  BAIDU_SCOPED_LOCK(mutex_);

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
  BAIDU_SCOPED_LOCK(mutex_);
  return DoIsTrained();
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
