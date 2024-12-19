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

#include "vector/vector_index_binary_ivf_flat.h"

#include <algorithm>
#include <climits>
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
#include "faiss/Index.h"
#include "faiss/IndexBinary.h"
#include "faiss/IndexBinaryFlat.h"
#include "faiss/IndexBinaryIVF.h"
#include "faiss/MetricType.h"
#include "faiss/impl/AuxIndexStructures.h"
#include "faiss/index_io.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "vector/vector_index_utils.h"

namespace dingodb {
DEFINE_int64(binary_ivf_flat_need_save_count, 10000, "binary ivf flat need save count");

bvar::LatencyRecorder g_binary_ivf_flat_upsert_latency("dingo_binary_ivf_flat_upsert_latency");
bvar::LatencyRecorder g_binary_ivf_flat_search_latency("dingo_binary_ivf_flat_search_latency");
bvar::LatencyRecorder g_binary_ivf_flat_range_search_latency("dingo_binary_ivf_flat_range_search_latency");
bvar::LatencyRecorder g_binary_ivf_flat_delete_latency("dingo_binary_ivf_flat_delete_latency");
bvar::LatencyRecorder g_binary_ivf_flat_load_latency("dingo_binary_ivf_flat_load_latency");
bvar::LatencyRecorder g_binary_ivf_flat_train_latency("dingo_binary_ivf_flat_train_latency");

VectorIndexBinaryIvfFlat::VectorIndexBinaryIvfFlat(int64_t id,
                                                   const pb::common::VectorIndexParameter& vector_index_parameter,
                                                   const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                                   ThreadPoolPtr thread_pool)
    : VectorIndex(id, vector_index_parameter, epoch, range, thread_pool) {
  metric_type_ = vector_index_parameter.binary_ivf_flat_parameter().metric_type();
  dimension_ = vector_index_parameter.binary_ivf_flat_parameter().dimension();

  nlist_org_ = vector_index_parameter.binary_ivf_flat_parameter().ncentroids() > 0
                   ? vector_index_parameter.binary_ivf_flat_parameter().ncentroids()
                   : Constant::kCreateBinaryIvfFlatParamNcentroids;

  nlist_ = nlist_org_;

  train_data_size_ = 0;
}

VectorIndexBinaryIvfFlat::~VectorIndexBinaryIvfFlat() = default;
butil::Status VectorIndexBinaryIvfFlat::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                    bool is_upsert) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckBinaryVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  const auto& ids = VectorIndexUtils::ExtractVectorId(vector_with_ids);
  const auto& vector_values = VectorIndexUtils::ExtractBinaryVectorValue(vector_with_ids, dimension_);

  BvarLatencyGuard bvar_guard(&g_binary_ivf_flat_upsert_latency);
  RWLockWriteGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_TRAIN, fmt::format("not train"));
  }

  if (is_upsert) {
    faiss::IDSelectorBatch sel(vector_with_ids.size(), ids.get());
    index_->remove_ids(sel);
  }
  index_->add_with_ids(vector_with_ids.size(), vector_values.get(), ids.get());

  return butil::Status::OK();
}

butil::Status VectorIndexBinaryIvfFlat::AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                                           bool is_upsert) {
  auto status = AddOrUpsert(vector_with_ids, is_upsert);
  if (BAIDU_UNLIKELY(pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code())) {
    status = Train(vector_with_ids);
    if (!status.ok()) {
      return status;
    }

    // try again
    status = AddOrUpsert(vector_with_ids, is_upsert);
    if (!status.ok()) {
      return status;
    }
  }

  return status;
}

butil::Status VectorIndexBinaryIvfFlat::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, true);
}

butil::Status VectorIndexBinaryIvfFlat::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  return AddOrUpsertWrapper(vector_with_ids, false);
}

butil::Status VectorIndexBinaryIvfFlat::Delete(const std::vector<int64_t>& delete_ids) {
  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  const auto& ids = VectorIndexUtils::CastVectorId(delete_ids);
  faiss::IDSelectorBatch sel(delete_ids.size(), ids.get());

  {
    BvarLatencyGuard bvar_guard(&g_binary_ivf_flat_delete_latency);
    RWLockWriteGuard guard(&rw_lock_);

    if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
      DINGO_LOG(WARNING) << fmt::format(
          "[vector_index.binary_ivf_flat][id({})] binary_ivf_flat not train, train first.", Id());
      return butil::Status::OK();
    }

    auto remove_count = index_->remove_ids(sel);
    if (0 == remove_count) {
      DINGO_LOG(WARNING) << fmt::format("[vector_index.binary_ivf_flat][id({})] remove not found vector id.", Id());
      return butil::Status(pb::error::Errno::EVECTOR_INVALID, "remove not found vector id");
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexBinaryIvfFlat::Search(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                               uint32_t topk,
                                               const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool,
                                               const pb::common::VectorSearchParameter& parameter,
                                               std::vector<pb::index::VectorWithDistanceResult>& results) {  // NOLINT

  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  if (topk <= 0) return butil::Status::OK();

  auto status = VectorIndexUtils::CheckBinaryVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  int32_t nprobe = parameter.binary_ivf_flat().nprobe() > 0 ? parameter.binary_ivf_flat().nprobe()
                                                            : Constant::kSearchBinaryIvfFlatParamNprobe;

  std::vector<faiss::IndexBinary::distance_t> distances;
  distances.resize(topk * vector_with_ids.size(), 0);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk * vector_with_ids.size(), -1);

  const auto& vector_values = VectorIndexUtils::ExtractBinaryVectorValue(vector_with_ids, dimension_);

  {
    BvarLatencyGuard bvar_guard(&g_binary_ivf_flat_search_latency);
    RWLockReadGuard guard(&rw_lock_);

    // Not trained(no data), direct return blank.
    if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
      results.resize(vector_with_ids.size(), pb::index::VectorWithDistanceResult());
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
      auto binary_ivf_flat_filter = filters.empty() ? nullptr : std::make_shared<BinaryIvfFlatIDSelector>(filters);
      ivf_search_parameters.sel = binary_ivf_flat_filter.get();
      index_->search(vector_with_ids.size(), vector_values.get(), topk, distances.data(), labels.data(),
                     &ivf_search_parameters);
    } else {
      index_->search(vector_with_ids.size(), vector_values.get(), topk, distances.data(), labels.data(),
                     &ivf_search_parameters);
    }
  }

  VectorIndexUtils::FillSearchBinaryResult(vector_with_ids, topk, distances, labels, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.binary_ivf_flat][id({})] result size {}", Id(), results.size());

  return butil::Status::OK();
}

butil::Status VectorIndexBinaryIvfFlat::RangeSearch(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
    const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool /*reconstruct*/,
    const pb::common::VectorSearchParameter& parameter, std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (vector_with_ids.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "vector_with_ids is empty");
  }
  auto status = VectorIndexUtils::CheckBinaryVectorDimension(vector_with_ids, dimension_);
  if (!status.ok()) {
    return status;
  }

  int32_t nprobe = parameter.binary_ivf_flat().nprobe() > 0 ? parameter.binary_ivf_flat().nprobe()
                                                            : Constant::kSearchBinaryIvfFlatParamNprobe;

  const auto& vector_values = VectorIndexUtils::ExtractBinaryVectorValue(vector_with_ids, dimension_);

  std::unique_ptr<faiss::RangeSearchResult> range_search_result =
      std::make_unique<faiss::RangeSearchResult>(vector_with_ids.size());

  {
    BvarLatencyGuard bvar_guard(&g_binary_ivf_flat_range_search_latency);
    RWLockReadGuard guard(&rw_lock_);

    // Not trained(no data), direct return blank.
    if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
      results.resize(vector_with_ids.size(), pb::index::VectorWithDistanceResult());
      return butil::Status::OK();
    }

    if (BAIDU_UNLIKELY(nprobe <= 0)) {
      nprobe = index_->nprobe;
    }

    // Prevent users from passing parameters out of bounds.
    nprobe = std::min(nprobe, static_cast<int32_t>(index_->nlist));

    faiss::IVFSearchParameters binary_ivf_search_parameters;
    binary_ivf_search_parameters.nprobe = nprobe;
    binary_ivf_search_parameters.max_codes = 0;
    binary_ivf_search_parameters.quantizer_params = nullptr;  // search for nlist . ignore

    try {
      if (!filters.empty()) {
        auto binary_ivf_flat_filter = filters.empty() ? nullptr : std::make_shared<BinaryIvfFlatIDSelector>(filters);
        binary_ivf_search_parameters.sel = binary_ivf_flat_filter.get();
        index_->range_search(vector_with_ids.size(), vector_values.get(), radius, range_search_result.get(),
                             &binary_ivf_search_parameters);
      } else {
        index_->range_search(vector_with_ids.size(), vector_values.get(), radius, range_search_result.get(),
                             &binary_ivf_search_parameters);
      }

    } catch (std::exception& e) {
      std::string s = fmt::format("range search exception: {}", e.what());
      DINGO_LOG(ERROR) << fmt::format("[vector_index.binary_ivf_flat][id({})] {}", Id(), s);
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
  }

  VectorIndexUtils ::FillRangeSearchResult(range_search_result, metric_type_, dimension_, results);

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.binary_ivf_flat][id({})] result size {}", Id(), results.size());

  return butil::Status::OK();
}

void VectorIndexBinaryIvfFlat::LockWrite() { rw_lock_.LockWrite(); }

void VectorIndexBinaryIvfFlat::UnlockWrite() { rw_lock_.UnlockWrite(); }

bool VectorIndexBinaryIvfFlat::SupportSave() { return true; }

butil::Status VectorIndexBinaryIvfFlat::Save(const std::string& path) {
  // Warning : read me first !!!!
  // Currently, the save function is executed in the fork child process.
  // When calling glog,
  // the child process will hang.
  // Remove glog temporarily.
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  try {
    faiss::write_index_binary(index_.get(), path.c_str());
  } catch (std::exception& e) {
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("write index exception: {}", e.what()));
  }

  return butil::Status();
}

butil::Status VectorIndexBinaryIvfFlat::Load(const std::string& path) {
  if (path.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "path is empty");
  }

  BvarLatencyGuard bvar_guard(&g_binary_ivf_flat_load_latency);

  faiss::IndexBinary* internal_raw_index = nullptr;
  try {
    internal_raw_index = faiss::read_index_binary(path.c_str(), 0);

  } catch (std::exception& e) {
    delete internal_raw_index;
    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("read index exception: {} {}", path, e.what()));
  }

  faiss::IndexBinaryIVF* internal_index = dynamic_cast<faiss::IndexBinaryIVF*>(internal_raw_index);
  if (BAIDU_UNLIKELY(!internal_index)) {
    if (internal_raw_index) {
      delete internal_raw_index;
      internal_raw_index = nullptr;
    }

    return butil::Status(pb::error::Errno::EINTERNAL, fmt::format("type cast failed"));
  }

  // avoid mem leak!!!
  std::unique_ptr<faiss::IndexBinaryIVF> internal_index_ivf_flat(internal_index);

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
    case pb::common::METRIC_TYPE_HAMMING: {
      // faiss::MetricType not support HAMMING ,L2 means min-heap, MetricType just for sort by heap
      if (BAIDU_UNLIKELY(internal_index->metric_type != faiss::MetricType::METRIC_L2)) {
        std::string s = fmt::format("metric type not match, {} L2 ", static_cast<int>(internal_index->metric_type));
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
    return butil::Status(pb::error::Errno::EINTERNAL,
                         fmt::format("nlist not match: {} {} {}", internal_index->nlist, nlist_, nlist_org_));
  }

  quantizer_.reset();
  index_ = std::move(internal_index_ivf_flat);

  nlist_ = index_->nlist;
  train_data_size_ = index_->ntotal;

  DINGO_LOG(INFO) << fmt::format("[vector_index.binary_ivf_flat][id({})] load finsh, path: {}", Id(), path);

  return butil::Status::OK();
}

int32_t VectorIndexBinaryIvfFlat::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexBinaryIvfFlat::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexBinaryIvfFlat::GetCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);

  if (IsTrainedImpl()) {
    count = index_->ntotal;
  } else {
    count = 0;
  }
  return butil::Status::OK();
}

butil::Status VectorIndexBinaryIvfFlat::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexBinaryIvfFlat::GetMemorySize(int64_t& memory_size) {
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

  memory_size = count * sizeof(faiss::idx_t) + count * dimension_ / CHAR_BIT * sizeof(faiss::IndexBinary::component_t) +
                nlist_ * dimension_ / CHAR_BIT * sizeof(faiss::IndexBinary::component_t);
  return butil::Status::OK();
}

bool VectorIndexBinaryIvfFlat::IsExceedsMaxElements() { return false; }

butil::Status VectorIndexBinaryIvfFlat::Train(std::vector<uint8_t>& train_datas) {
  size_t data_size = train_datas.size() / (dimension_ / CHAR_BIT);
  if (data_size == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "data size invalid");
  }
  if (train_datas.size() % (dimension_ / CHAR_BIT) != 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
                         fmt::format("dimension not match {} * CHAR_BIT(8) {}", train_datas.size(), dimension_));
  }

  faiss::ClusteringParameters clustering_parameters;
  if (BAIDU_UNLIKELY(data_size < (clustering_parameters.min_points_per_centroid * nlist_))) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.binary_ivf_flat][id({})] train_datas size({}) not enough, suggest at least {}.", Id(), data_size,
        clustering_parameters.min_points_per_centroid * nlist_);
  } else if (BAIDU_UNLIKELY(data_size >= clustering_parameters.min_points_per_centroid * nlist_ &&
                            data_size < clustering_parameters.max_points_per_centroid * nlist_)) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.binary_ivf_flat][id({})] train_datas size({}) not enough, suggest at least {}.", Id(), data_size,
        clustering_parameters.max_points_per_centroid * nlist_);
  }

  BvarLatencyGuard bvar_guard(&g_binary_ivf_flat_train_latency);
  RWLockWriteGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(IsTrainedImpl())) {
    return butil::Status::OK();
  }

  DINGO_LOG(INFO) << fmt::format("[vector_index.binary_ivf_flat][id({})] train size: {}.", Id(), data_size);

  // critical code
  if (BAIDU_UNLIKELY(data_size < nlist_)) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.binary_ivf_flat][id({})] data size({}) too small, nlist({}) degenerate to 1.", Id(), data_size,
        nlist_);
    nlist_ = 1;
  }

  // init index
  Init();

  train_data_size_ = 0;

  try {
    index_->train(data_size, train_datas.data());
    if (!index_->is_trained) {
      Reset();
      return butil::Status(pb::error::Errno::EINTERNAL, "check binary_ivf_flat index train failed");
    }

  } catch (std::exception& e) {
    Reset();
    return butil::Status(pb::error::Errno::EINTERNAL, "train binary_ivf_flat exception");
  }

  train_data_size_ = data_size;

  return butil::Status::OK();
}

butil::Status VectorIndexBinaryIvfFlat::Train(const std::vector<pb::common::VectorWithId>& vectors) {
  std::vector<uint8_t> train_datas;
  train_datas.reserve((dimension_ / CHAR_BIT) * vectors.size());
  for (const auto& vector : vectors) {
    if (BAIDU_UNLIKELY((dimension_ / CHAR_BIT) != vector.vector().binary_values().size())) {
      std::string s =
          fmt::format("dimension not match {} * CHAR_BIT(8) {}", vector.vector().binary_values().size(), dimension_);
      return butil::Status(pb::error::Errno::EINTERNAL, s);
    }
    for (auto val : vector.vector().binary_values()) {
      uint8_t value = static_cast<uint8_t>(val[0]);
      train_datas.push_back(value);
    }
  }

  return VectorIndexBinaryIvfFlat::Train(train_datas);
}

bool VectorIndexBinaryIvfFlat::NeedToRebuild() {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
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

bool VectorIndexBinaryIvfFlat::IsTrained() {
  RWLockReadGuard guard(&rw_lock_);

  return IsTrainedImpl();
}

bool VectorIndexBinaryIvfFlat::NeedToSave(int64_t last_save_log_behind) {
  RWLockReadGuard guard(&rw_lock_);

  if (BAIDU_UNLIKELY(!IsTrainedImpl())) {
    return false;
  }

  if (index_->ntotal == 0) {
    return false;
  }

  if (last_save_log_behind > FLAGS_binary_ivf_flat_need_save_count) {
    return true;
  }

  return false;
}

void VectorIndexBinaryIvfFlat::Init() {
  quantizer_ = std::make_unique<faiss::IndexBinaryFlat>(dimension_);
  index_ = std::make_unique<faiss::IndexBinaryIVF>(quantizer_.get(), dimension_, nlist_);
}

bool VectorIndexBinaryIvfFlat::IsTrainedImpl() {
  bool is_trained = ((index_ && !quantizer_ && index_->own_fields) || (quantizer_ && index_ && !index_->own_fields))
                        ? index_->is_trained
                        : false;

  DINGO_LOG(DEBUG) << fmt::format("[vector_index.binary_ivf_flat][id({})] is train {}", Id(), is_trained);
  return is_trained;
}

void VectorIndexBinaryIvfFlat::Reset() {
  quantizer_->reset();
  index_->reset();
  nlist_ = nlist_org_;
}

}  // namespace dingodb
