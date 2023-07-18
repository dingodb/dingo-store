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
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "bthread/types.h"
#include "butil/status.h"
#include "common/logging.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"
#include "proto/region_control.pb.h"

namespace dingodb {

VectorIndexFlat::VectorIndexFlat(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
    : VectorIndex(id, vector_index_parameter) {
  bthread_mutex_init(&mutex_, nullptr);
  is_online_.store(true);

  metric_type_ = vector_index_parameter.flat_parameter().metric_type();
  dimension_ = vector_index_parameter.flat_parameter().dimension();

  if (pb::common::MetricType::METRIC_TYPE_L2 == metric_type_) {
    raw_index_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
  } else if (pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT == metric_type_) {
    raw_index_ = std::make_unique<faiss::IndexFlatIP>(dimension_);
  } else {
    DINGO_LOG(WARNING) << fmt::format("Flat : not support metric type : {} use L2 default",
                                      static_cast<int>(metric_type_));
    raw_index_ = std::make_unique<faiss::IndexFlatL2>(dimension_);
  }

  index_ = std::make_unique<faiss::IndexIDMap>(raw_index_.get());
}

VectorIndexFlat::~VectorIndexFlat() {
  index_->reset();
  bthread_mutex_destroy(&mutex_);
}

butil::Status VectorIndexFlat::AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                           bool is_upsert) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

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

  std::unique_ptr<faiss::idx_t> ids;
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

  std::unique_ptr<float> vectors;
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
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  std::unique_ptr<faiss::idx_t> ids;
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
                                      std::vector<pb::index::VectorWithDistanceResult>& results,
                                      [[maybe_unused]] bool reconstruct) {
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  if (vector_with_ids.empty()) {
    DINGO_LOG(WARNING) << "vector_with_ids is empty";
    return butil::Status::OK();
  }

  if (topk == 0) {
    DINGO_LOG(WARNING) << "topk is invalid";
    return butil::Status::OK();
  }

  if (vector_with_ids[0].vector().float_values_size() != this->dimension_) {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector dimension is not match, input=%d, index=%ld",
                         vector_with_ids[0].vector().float_values_size(), this->dimension_);
  }

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(topk * vector_with_ids.size(), 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk * vector_with_ids.size(), -1);

  std::unique_ptr<float> vectors;
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
    }
  }

  {
    BAIDU_SCOPED_LOCK(mutex_);
    index_->search(vector_with_ids.size(), vectors.get(), topk, distances.data(), labels.data());
  }

  for (size_t row = 0; row < vector_with_ids.size(); ++row) {
    auto& result = results.emplace_back();

    for (size_t i = 0; i < topk; i++) {
      if (labels[i] < 0) {
        continue;
      }
      auto* vector_with_distance = result.add_vector_with_distances();

      auto* vector_with_id = vector_with_distance->mutable_vector_with_id();
      vector_with_id->set_id(labels[i]);
      vector_with_id->mutable_vector()->set_dimension(dimension_);
      vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
      vector_with_distance->set_distance(distances[i]);
      vector_with_distance->set_metric_type(metric_type_);
    }
  }

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::SetOffline() {
  is_online_.store(false);
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::SetOnline() {
  is_online_.store(true);
  return butil::Status::OK();
}

bool VectorIndexFlat::IsOnline() { return is_online_.load(); }

void VectorIndexFlat::LockWrite() { bthread_mutex_lock(&mutex_); }

void VectorIndexFlat::UnlockWrite() { bthread_mutex_unlock(&mutex_); }

butil::Status VectorIndexFlat::Save(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Flat index not support save");
}

butil::Status VectorIndexFlat::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Flat index not support load");
}

butil::Status VectorIndexFlat::GetCount([[maybe_unused]] uint64_t& count) {
  count = index_->id_map.size();
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                                             [[maybe_unused]] uint64_t last_save_log_behind) {
  need_to_rebuild = false;
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::NeedToSave([[maybe_unused]] bool& need_to_save,
                                          [[maybe_unused]] uint64_t last_save_log_behind) {
  need_to_save = false;
  return butil::Status::OK();
}

}  // namespace dingodb
