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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/logging.h"
#include "common/threadpool.h"
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

bvar::LatencyRecorder g_ivf_pq_upsert_latency("dingo_ivf_pq_upsert_latency");
bvar::LatencyRecorder g_ivf_pq_search_latency("dingo_ivf_pq_search_latency");
bvar::LatencyRecorder g_ivf_pq_range_search_latency("dingo_ivf_pq_range_search_latency");
bvar::LatencyRecorder g_ivf_pq_delete_latency("dingo_ivf_pq_delete_latency");
bvar::LatencyRecorder g_ivf_pq_load_latency("dingo_ivf_pq_load_latency");
bvar::LatencyRecorder g_ivf_pq_train_latency("dingo_ivf_pq_train_latency");

VectorIndexIvfPq::VectorIndexIvfPq(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                   const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                   ThreadPoolPtr thread_pool)
    : VectorIndex(id, vector_index_parameter, epoch, range, thread_pool), inner_index_type_(IndexTypeInIvfPq::kUnknow) {
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
}

VectorIndexIvfPq::~VectorIndexIvfPq() = default;

butil::Status VectorIndexIvfPq::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                   bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }

  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  BvarLatencyGuard bvar_guard(&g_ivf_pq_upsert_latency);
  {
    RWLockWriteGuard guard(&rw_lock_);
    status = InvokeConcreteFunction("AddOrUpsertWrapper",
                                    &VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>::AddOrUpsertWrapper,
                                    &VectorIndexRawIvfPq::AddOrUpsertWrapper, true, vector_with_ids, is_upsert);
    if (status.ok()) {
      return status;
    } else if (!status.ok() && pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code()) {
      return status;
    }
  }

  // train
  status = Train(vector_with_ids);
  if (!status.ok()) {
    return status;
  }

  // add again
  {
    RWLockWriteGuard guard(&rw_lock_);
    status = InvokeConcreteFunction("AddOrUpsertWrapper",
                                    &VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>::AddOrUpsertWrapper,
                                    &VectorIndexRawIvfPq::AddOrUpsertWrapper, true, vector_with_ids, is_upsert);
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
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  BvarLatencyGuard bvar_guard(&g_ivf_pq_delete_latency);
  RWLockWriteGuard guard(&rw_lock_);

  butil::Status status = InvokeConcreteFunction("Delete", &VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>::Delete,
                                                &VectorIndexRawIvfPq::Delete, false, delete_ids);
  if (!status.ok() && (pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code())) {
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                       const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                                       const pb::common::VectorSearchParameter& parameter,
                                       std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  if (topk <= 0) return butil::Status::OK();
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  BvarLatencyGuard bvar_guard(&g_ivf_pq_search_latency);
  RWLockReadGuard guard(&rw_lock_);
  status = InvokeConcreteFunction("Search", &VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>::Search,
                                  &VectorIndexRawIvfPq::Search, false, vector_with_ids, topk, filters, reconstruct,
                                  parameter, results);
  if (!status.ok() && (pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code())) {
    return status;
  }

  if (pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code()) {
    for (size_t row = 0; row < vector_with_ids.size(); ++row) {
      auto& result = results.emplace_back();
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                                            const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                            bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  BvarLatencyGuard bvar_guard(&g_ivf_pq_range_search_latency);
  RWLockReadGuard guard(&rw_lock_);

  status = InvokeConcreteFunction("RangeSearch", &VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>::RangeSearch,
                                  &VectorIndexRawIvfPq::RangeSearch, false, vector_with_ids, radius, filters,
                                  reconstruct, parameter, results);
  if (!status.ok() && (pb::error::Errno::EVECTOR_NOT_TRAIN != status.error_code())) {
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
  butil::Status status = InvokeConcreteFunction("Save", &VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>::Save,
                                                &VectorIndexRawIvfPq::Save, false, path);
  if (!status.ok()) {
    // DINGO_LOG(ERROR) << "VectorIndexIvfPq::Save failed " << status.error_cstr();
    return status;
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::Load(const std::string& path) {
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  butil::Status status;

  BvarLatencyGuard bvar_guard(&g_ivf_pq_load_latency);

  // first ivf pq
  auto internal_index_raw_ivf_pq =
      std::make_unique<VectorIndexRawIvfPq>(id, vector_index_parameter, epoch, range, thread_pool);

  status = internal_index_raw_ivf_pq->Load(path);
  if (status.ok()) {
    inner_index_type_ = IndexTypeInIvfPq::kIvfPq;
    index_raw_ivf_pq_ = std::move(internal_index_raw_ivf_pq);
    index_flat_ = nullptr;
    return status;
  }

  DINGO_LOG(WARNING) << fmt::format("[vector_index.ivf_pq][id({})] load index failed, error: {} path: {}", Id(),
                                    status.error_cstr(), path);

  // try flat again
  pb::common::VectorIndexParameter index_parameter_flat;
  index_parameter_flat.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
  ::dingodb::pb::common::CreateFlatParam* flat_parameter = index_parameter_flat.mutable_flat_parameter();
  flat_parameter->set_metric_type(vector_index_parameter.ivf_pq_parameter().metric_type());
  flat_parameter->set_dimension(vector_index_parameter.ivf_pq_parameter().dimension());
  auto internal_index_flat = std::make_unique<VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>>(
      id, index_parameter_flat, epoch, range, thread_pool);

  status = internal_index_flat->Load(path);
  if (status.ok()) {
    inner_index_type_ = IndexTypeInIvfPq::kFlat;
    index_flat_ = std::move(internal_index_flat);
    index_raw_ivf_pq_ = nullptr;
    return status;
  }

  DINGO_LOG(WARNING) << fmt::format("[vector_index.ivf_pq][id({})] load index failed, error: {} path: {}", Id(),
                                    status.error_cstr(), path);
  return status;
}

int32_t VectorIndexIvfPq::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexIvfPq::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexIvfPq::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (IsTrainedImpl()) {
    switch (inner_index_type_) {
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

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    memory_size = 0;
    return butil::Status::OK();
  }

  switch (inner_index_type_) {
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

bool VectorIndexIvfPq::IsExceedsMaxElements(int64_t /*vector_size*/) { return false; }

butil::Status VectorIndexIvfPq::Train(std::vector<float>& train_datas) {
  size_t data_size = train_datas.size() / dimension_;
  if (data_size == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "data size invalid");
  }
  if (train_datas.size() % dimension_ != 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("dimension not match {} {}", train_datas.size(), dimension_));
  }

  faiss::ClusteringParameters clustering_parameters;

  faiss::idx_t train_nlist_size = clustering_parameters.max_points_per_centroid * nlist_;

  faiss::ProductQuantizer pq = faiss::ProductQuantizer(dimension_, nsubvector_, nbits_per_idx_);

  faiss::idx_t train_subvector_size = pq.cp.max_points_per_centroid * (1 << nbits_per_idx_);

  BvarLatencyGuard bvar_guard(&g_ivf_pq_train_latency);
  RWLockWriteGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(IsTrainedImpl())) {
    return butil::Status::OK();
  }

  inner_index_type_ = data_size >= std::max(train_nlist_size, train_subvector_size) ? IndexTypeInIvfPq::kIvfPq
                                                                                    : IndexTypeInIvfPq::kFlat;
  DINGO_LOG(INFO) << fmt::format("[vector_index.ivf_pq][id({})] train index type: {} data_size: {}", Id(),
                                 static_cast<int>(inner_index_type_), data_size);

  // init index
  Init();

  butil::Status status;
  switch (inner_index_type_) {
    case IndexTypeInIvfPq::kFlat: {
      status = index_flat_->Train(train_datas);
      if (!status.ok()) {
        Reset();
        return status;
      }
      if (!index_flat_->IsTrained()) {
        Reset();
        return butil::Status(pb::error::Errno::EINTERNAL, "check flat index train failed");
      }
      break;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      status = index_raw_ivf_pq_->Train(train_datas);
      if (!status.ok()) {
        Reset();
        return status;
      }
      if (!index_raw_ivf_pq_->IsTrained()) {
        Reset();
        return butil::Status(pb::error::Errno::EINTERNAL, "check raw_ivf_pq index train failed");
      }
      break;
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      Reset();
      return butil::Status(pb::error::Errno::EINTERNAL, "unknown inner index type");
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexIvfPq::Train(const std::vector<pb::common::VectorWithId>& vectors) {
  std::vector<float> train_datas;
  train_datas.reserve(dimension_ * vectors.size());
  for (const auto& vector : vectors) {
    if (BAIDU_UNLIKELY(dimension_ != vector.vector().float_values().size())) {
      std::string s = fmt::format("dimension not match {} {}", vector.vector().float_values().size(), dimension_);
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
    train_datas.insert(train_datas.end(), vector.vector().float_values().begin(), vector.vector().float_values().end());
  }

  return VectorIndexIvfPq::Train(train_datas);
}

bool VectorIndexIvfPq::NeedToRebuild() {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return false;
  }

  switch (inner_index_type_) {
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
  return IsTrainedImpl();
}

bool VectorIndexIvfPq::NeedToSave(int64_t last_save_log_behind) {
  RWLockReadGuard guard(&rw_lock_);
  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return false;
  }

  switch (inner_index_type_) {
    case IndexTypeInIvfPq::kFlat: {
      return index_flat_->NeedToSave(last_save_log_behind);
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return index_raw_ivf_pq_->NeedToSave(last_save_log_behind);
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.ivf_pq][id({})] unknown index type.", Id());
      return false;
    }
  }

  return false;
}

pb::common::VectorIndexType VectorIndexIvfPq::VectorIndexSubType() {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
  }

  switch (inner_index_type_) {
    case IndexTypeInIvfPq::kFlat: {
      return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ;
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      return pb::common::VectorIndexType::VECTOR_INDEX_TYPE_NONE;
    }
  }
}

void VectorIndexIvfPq::Init() {
  if (IndexTypeInIvfPq::kFlat == inner_index_type_) {
    pb::common::VectorIndexParameter index_parameter_flat;
    index_parameter_flat.set_vector_index_type(::dingodb::pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
    ::dingodb::pb::common::CreateFlatParam* flat_parameter = index_parameter_flat.mutable_flat_parameter();
    flat_parameter->set_metric_type(vector_index_parameter.ivf_pq_parameter().metric_type());
    flat_parameter->set_dimension(vector_index_parameter.ivf_pq_parameter().dimension());
    index_flat_ = std::make_unique<VectorIndexFlat<faiss::Index, faiss::IndexIDMap2>>(id, index_parameter_flat, epoch,
                                                                                      range, thread_pool);

  } else if (IndexTypeInIvfPq::kIvfPq == inner_index_type_) {
    index_raw_ivf_pq_ = std::make_unique<VectorIndexRawIvfPq>(id, vector_index_parameter, epoch, range, thread_pool);
  } else {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.ivf_pq][id({})] unknown index type.", Id());
  }
}

bool VectorIndexIvfPq::IsTrainedImpl() {
  bool is_trained = false;
  switch (inner_index_type_) {
    case IndexTypeInIvfPq::kFlat: {
      is_trained = index_flat_->IsTrained();
      break;
    }
    case IndexTypeInIvfPq::kIvfPq: {
      is_trained = index_raw_ivf_pq_->IsTrained();
      break;
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default:
      is_trained = false;
  }

  return is_trained;
}

void VectorIndexIvfPq::Reset() {
  inner_index_type_ = IndexTypeInIvfPq::kUnknow;
  index_flat_.reset();
  index_raw_ivf_pq_.reset();
}

template <typename FLAT_FUNC_PTR, typename PQ_FUNC_PTR, typename... Args>
butil::Status VectorIndexIvfPq::InvokeConcreteFunction(const char* name, FLAT_FUNC_PTR flat_func_ptr,
                                                       PQ_FUNC_PTR pq_func_ptr, bool use_glog, Args&&... args) {
  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    std::string s = fmt::format("{} not train, train first.", name);
    DINGO_LOG_IF(ERROR, use_glog) << fmt::format("[vector_index.ivf_pq][id({})] {}", Id(), s);
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
  }

  switch (inner_index_type_) {
    case IndexTypeInIvfPq::kFlat: {
      return ((*index_flat_).*flat_func_ptr)(std::forward<Args>(args)...);
    }
    case IndexTypeInIvfPq::kIvfPq: {
      return ((*index_raw_ivf_pq_).*pq_func_ptr)(std::forward<Args>(args)...);
    }
    case IndexTypeInIvfPq::kUnknow:
      [[fallthrough]];
    default: {
      std::string s = fmt::format("{} unknown index type.", name);
      DINGO_LOG_IF(ERROR, use_glog) << fmt::format("[vector_index.ivf_pq][id({})] {}", Id(), s);
      return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, s);
    }
  }

  return butil::Status::OK();
}

}  // namespace dingodb
