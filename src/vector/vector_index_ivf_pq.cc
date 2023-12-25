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

#include "vector/vector_index_ivf_pq.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bthread/types.h"
#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "faiss/impl/ProductQuantizer.h"
#include "faiss/index_io.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/debug.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_utils.h"

namespace dingodb {

VectorIndexIvfPq::VectorIndexIvfPq(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                   const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, epoch, range), index_type_in_ivf_pq_(IndexTypeInIvfPq::kUnknow) {
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

  // Delay object creation.
}

VectorIndexIvfPq::~VectorIndexIvfPq() = default;

butil::Status VectorIndexIvfPq::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                   bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status();
  }

  butil::Status status;
  {
    RWLockWriteGuard guard(&rw_lock_);
    status = InvokeConcreteFunction("AddOrUpsertWrapper", &VectorIndexFlat::AddOrUpsertWrapper,
                                    &VectorIndexRawIvfPq::AddOrUpsertWrapper, true, vector_with_ids, is_upsert);
  }

  if (status.ok()) {
    return status;
  }

  if (!status.ok() && pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code()) {
    DINGO_LOG(ERROR) << "VectorIndexIvfPq::AddOrUpsertWrapper failed " << status.error_cstr();
    return status;
  }

  // train
  status = Train(vector_with_ids);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << "VectorIndexIvfPq::Train failed " << status.error_cstr();
    return status;
  }

  // add again
  {
    RWLockWriteGuard guard(&rw_lock_);
    status = InvokeConcreteFunction("AddOrUpsertWrapper", &VectorIndexFlat::AddOrUpsertWrapper,
                                    &VectorIndexRawIvfPq::AddOrUpsertWrapper, true, vector_with_ids, is_upsert);
  }

  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
  }

  return status;
}

butil::Status VectorIndexIvfPq::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexIvfPq::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexIvfPq::Delete(const std::vector<int64_t>& delete_ids) {
  RWLockWriteGuard guard(&rw_lock_);
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  butil::Status status =
      InvokeConcreteFunction("Delete", &VectorIndexFlat::Delete, &VectorIndexRawIvfPq::Delete, false, delete_ids);
  if (!status.ok() && (pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code())) {
    DINGO_LOG(ERROR) << "VectorIndexIvfPq::Delete failed " << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                       std::vector<std::shared_ptr<FilterFunctor>> filters, bool reconstruct,
                                       const pb::common::VectorSearchParameter& parameter,
                                       std::vector<pb::index::VectorWithDistanceResult>& results) {
  RWLockReadGuard guard(&rw_lock_);
  butil::Status status = InvokeConcreteFunction("Search", &VectorIndexFlat::Search, &VectorIndexRawIvfPq::Search, false,
                                                vector_with_ids, topk, filters, reconstruct, parameter, results);
  if (!status.ok() && (pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code())) {
    DINGO_LOG(ERROR) << "VectorIndexIvfPq::Search failed " << status.error_cstr();
    return status;
  }

  if (pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code()) {
    for (size_t row = 0; row < vector_with_ids.size(); ++row) {
      auto& result = results.emplace_back();
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::RangeSearch(std::vector<pb::common::VectorWithId> vector_with_ids, float radius,
                                            std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                            bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) {
  RWLockReadGuard guard(&rw_lock_);
  butil::Status status =
      InvokeConcreteFunction("RangeSearch", &VectorIndexFlat::RangeSearch, &VectorIndexRawIvfPq::RangeSearch, false,
                             vector_with_ids, radius, filters, reconstruct, parameter, results);
  if (!status.ok() && (pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code())) {
    DINGO_LOG(ERROR) << "VectorIndexIvfPq::RangeSearch failed " << status.error_cstr();
    return status;
  }

  if (pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code()) {
    for (size_t row = 0; row < vector_with_ids.size(); ++row) {
      auto& result = results.emplace_back();
    }
  }
  return butil::Status::OK();
}

void VectorIndexIvfPq::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexIvfPq::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexIvfPq::SupportSave() { return true; }

butil::Status VectorIndexIvfPq::Save(const std::string& path) {
  // Warning : read me first !!!!
  // Currently, the save function is executed in the fork child process.
  // When calling glog,
  // the child process will hang.
  // Remove glog temporarily.
  // The outside has been locked. Remove the locking operation here.
  // BAIDU_SCOPED_LOCK(mutex_);
  butil::Status status =
      InvokeConcreteFunction("Save", &VectorIndexFlat::Save, &VectorIndexRawIvfPq::Save, false, path);
  if (!status.ok()) {
    // DINGO_LOG(ERROR) << "VectorIndexIvfPq::Save failed " << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::Load(const std::string& path) {
  butil::Status status;
  std::string error_message;

  // The outside has been locked. Remove the locking operation here.
  // BAIDU_SCOPED_LOCK(mutex_);

  // first ivf pq
  auto internal_index_type_in_ivf_pq = IndexTypeInIvfPq::kIvfPq;
  auto internal_index_raw_ivf_pq = std::make_unique<VectorIndexRawIvfPq>(id, vector_index_parameter, epoch, range);

  status = internal_index_raw_ivf_pq->Load(path);
  if (status.ok()) {
    index_type_in_ivf_pq_ = internal_index_type_in_ivf_pq;
    index_raw_ivf_pq_ = std::move(internal_index_raw_ivf_pq);
    index_flat_ = nullptr;
    return status;
  } else {
    std::string s = fmt::format("VectorIndexRawIvfPq::Load failed : {} path:{}", status.error_cstr(), path);
    DINGO_LOG(WARNING) << s;
    internal_index_raw_ivf_pq = nullptr;
    internal_index_type_in_ivf_pq = IndexTypeInIvfPq::kUnknow;
    error_message = s;
  }

  // try flat again
  internal_index_type_in_ivf_pq = IndexTypeInIvfPq::kFlat;

  pb::common::VectorIndexParameter index_parameter_flat;
  index_parameter_flat.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  ::dingodb::pb::common::CreateFlatParam* flat_parameter = index_parameter_flat.mutable_flat_parameter();
  flat_parameter->set_metric_type(vector_index_parameter.ivf_pq_parameter().metric_type());
  flat_parameter->set_dimension(vector_index_parameter.ivf_pq_parameter().dimension());
  auto internal_index_flat = std::make_unique<VectorIndexFlat>(id, index_parameter_flat, epoch, range);

  status = internal_index_flat->Load(path);
  if (status.ok()) {
    index_type_in_ivf_pq_ = internal_index_type_in_ivf_pq;
    index_flat_ = std::move(internal_index_flat);
    index_raw_ivf_pq_ = nullptr;
    return status;
  } else {
    std::string s = fmt::format("VectorIndexFlat::Load failed : {} path:{}", status.error_cstr(), path);
    DINGO_LOG(WARNING) << s;
    internal_index_flat = nullptr;
    internal_index_type_in_ivf_pq = IndexTypeInIvfPq::kUnknow;
    return butil::Status(pb::error::Errno::EINTERNAL, error_message + s);
  }

  return butil::Status::OK();
}

int32_t VectorIndexIvfPq::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexIvfPq::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexIvfPq::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (DoIsTrained()) {
    switch (index_type_in_ivf_pq_) {
      case IndexTypeInIvfPq::kFlat: {
        return index_flat_->GetCount(count);
      }
      case IndexTypeInIvfPq::kIvfPq: {
        return index_raw_ivf_pq_->GetCount(count);
      }
      case IndexTypeInIvfPq::kUnknow:
        [[fallthrough]];
      default: {
        count = 0;
      }
    }
  } else {
    count = 0;
  }
  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::GetMemorySize(int64_t& memory_size) {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    memory_size = 0;
    return butil::Status::OK();
  }

  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      return index_flat_->GetMemorySize(memory_size);
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return index_raw_ivf_pq_->GetMemorySize(memory_size);
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      memory_size = 0;
      break;
    }
  }

  return butil::Status::OK();
}

bool VectorIndexIvfPq::IsExceedsMaxElements() { return false; }

butil::Status VectorIndexIvfPq::Train(const std::vector<float>& train_datas) {
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

  faiss::idx_t train_nlist_size = clustering_parameters.max_points_per_centroid * nlist_;

  faiss::ProductQuantizer pq = faiss::ProductQuantizer(dimension_, nsubvector_, nbits_per_idx_);

  faiss::idx_t train_subvector_size = pq.cp.max_points_per_centroid * (1 << nbits_per_idx_);

  RWLockWriteGuard guard(&rw_lock_);
  if (BAIDU_UNLIKELY(DoIsTrained())) {
    std::string s = fmt::format("already trained . ignore");
    DINGO_LOG(WARNING) << s;
    return butil::Status::OK();
  }

  if (data_size >= std::max(train_nlist_size, train_subvector_size)) {
    index_type_in_ivf_pq_ = IndexTypeInIvfPq::kIvfPq;
  } else {
    index_type_in_ivf_pq_ = IndexTypeInIvfPq::kFlat;
  }

  // init index
  Init();

  butil::Status status;

  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      status = index_flat_->Train(train_datas);
      if (!status.ok()) {
        Reset();
        std::string s = fmt::format("VectorIndexFlat::Train failed . {}", status.error_cstr());
        DINGO_LOG(ERROR) << s;
        return status;
      }
      break;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      status = index_raw_ivf_pq_->Train(train_datas);
      if (!status.ok()) {
        Reset();
        std::string s = fmt::format("VectorIndexRawIvfPq::Train failed . {}", status.error_cstr());
        DINGO_LOG(ERROR) << s;
        return status;
      }
      break;
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      Reset();
      std::string s = fmt::format("Invalid IndexTypeInIvfPq type :  {}", static_cast<int>(index_type_in_ivf_pq_));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  // double check
  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      if (BAIDU_UNLIKELY(!index_flat_->IsTrained())) {
        Reset();
        std::string s =
            fmt::format("flat train failed. data size : {} dimension : {}. internal error", data_size, dimension_);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      if (BAIDU_UNLIKELY(!index_raw_ivf_pq_->IsTrained())) {
        Reset();
        std::string s = fmt::format("raw ivf pq train failed. data size : {} dimension : {}. internal error", data_size,
                                    dimension_);
        DINGO_LOG(ERROR) << s;
        return butil::Status(pb::error::Errno::EINTERNAL, s);
      }
      break;
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      Reset();
      std::string s = fmt::format("Invalid IndexTypeInIvfPq type :  {}", static_cast<int>(index_type_in_ivf_pq_));
      DINGO_LOG(ERROR) << s;
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) {
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

  return VectorIndexIvfPq::Train(train_datas);
}

bool VectorIndexIvfPq::NeedToRebuild() {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("not trained");
    DINGO_LOG(WARNING) << s;
    return false;
  }

  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      faiss::ClusteringParameters clustering_parameters;
      faiss::idx_t train_nlist_size = clustering_parameters.max_points_per_centroid * nlist_;
      faiss::ProductQuantizer pq = faiss::ProductQuantizer(dimension_, nsubvector_, nbits_per_idx_);
      faiss::idx_t train_subvector_size = pq.cp.max_points_per_centroid * (1 << nbits_per_idx_);
      auto data_size = std::max(train_nlist_size, train_subvector_size);
      int64_t count = 0;
      index_flat_->GetCount(count);
      if (count >= data_size) {
        return true;
      }
      break;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return index_raw_ivf_pq_->NeedToRebuild();
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      break;
    }
  }

  return false;
}

bool VectorIndexIvfPq::IsTrained() {
  RWLockReadGuard guard(&rw_lock_);
  return DoIsTrained();
}

bool VectorIndexIvfPq::NeedToSave(int64_t last_save_log_behind) {
  RWLockReadGuard guard(&rw_lock_);
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("ivf pq not train. train first.");
    DINGO_LOG(ERROR) << s;
    return false;
  }

  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      return index_flat_->NeedToSave(last_save_log_behind);
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return index_raw_ivf_pq_->NeedToSave(last_save_log_behind);
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("ivf pq not train. train first.");
      DINGO_LOG(ERROR) << s;
      return false;
    }
  }

  return false;
}

pb::common::VectorIndexType VectorIndexIvfPq::VectorIndexSubType() {
  RWLockReadGuard guard(&rw_lock_);
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("ivf pq not train. train first.");
    DINGO_LOG(ERROR) << s;
    return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
  }

  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ;
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("ivf pq not train. train first.");
      DINGO_LOG(ERROR) << s;
      return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
    }
  }
}

void VectorIndexIvfPq::Init() {
  if (IndexTypeInIvfPq::kFlat == index_type_in_ivf_pq_) {
    pb::common::VectorIndexParameter index_parameter_flat;
    index_parameter_flat.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    ::dingodb::pb::common::CreateFlatParam* flat_parameter = index_parameter_flat.mutable_flat_parameter();
    flat_parameter->set_metric_type(vector_index_parameter.ivf_pq_parameter().metric_type());
    flat_parameter->set_dimension(vector_index_parameter.ivf_pq_parameter().dimension());
    index_flat_ = std::make_unique<VectorIndexFlat>(id, index_parameter_flat, epoch, range);

  } else if (IndexTypeInIvfPq::kIvfPq == index_type_in_ivf_pq_) {
    index_raw_ivf_pq_ = std::make_unique<VectorIndexRawIvfPq>(id, vector_index_parameter, epoch, range);
  } else {
    DINGO_LOG(ERROR) << fmt::format("index_type_in_ivf_pq_ : {} . wrong  state.",
                                    static_cast<int>(index_type_in_ivf_pq_));
  }
}

bool VectorIndexIvfPq::DoIsTrained() {
  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      return index_flat_->IsTrained();
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return index_raw_ivf_pq_->IsTrained();
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      return false;
    }
  }
}

void VectorIndexIvfPq::Reset() {
  index_type_in_ivf_pq_ = IndexTypeInIvfPq::kUnknow;
  index_flat_.reset();
  index_raw_ivf_pq_.reset();
}

template <typename FLAT_FUNC_PTR, typename PQ_FUNC_PTR, typename... Args>
butil::Status VectorIndexIvfPq::InvokeConcreteFunction(const char* name, FLAT_FUNC_PTR flat_func_ptr,
                                                       PQ_FUNC_PTR pq_func_ptr, bool use_glog, Args&&... args) {
  if (BAIDU_UNLIKELY(!DoIsTrained())) {
    std::string s = fmt::format("{} : ivf pq not train. train first.", name);
    if (use_glog) DINGO_LOG(ERROR) << name << " : " << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
  }

  switch (index_type_in_ivf_pq_) {
    case IndexTypeInIvfPq::kFlat: {
      return ((*index_flat_).*flat_func_ptr)(std::forward<Args>(args)...);
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return ((*index_raw_ivf_pq_).*pq_func_ptr)(std::forward<Args>(args)...);
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("{} : ivf pq not train. train first.", name);
      if (use_glog) DINGO_LOG(ERROR) << name << " : " << s;
      return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
