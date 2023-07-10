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
#include "butil/status.h"
#include "common/logging.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

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

butil::Status VectorIndexFlat::Add(uint64_t id, const std::vector<float>& vector) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  // check
  if (vector.size() != static_cast<size_t>(dimension_)) {
    std::string s =
        fmt::format("Flat : float size : {} not equal to  dimension(create) : {}", vector.size(), dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  BAIDU_SCOPED_LOCK(mutex_);
  index_->add_with_ids(1, vector.data(), reinterpret_cast<faiss::idx_t*>(&id));
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Upsert(uint64_t id, const std::vector<float>& vector) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  // check
  if (vector.size() != static_cast<size_t>(dimension_)) {
    std::string s =
        fmt::format("Flat : float size : {} not equal to  dimension(create) : {}", vector.size(), dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  std::array<faiss::idx_t, 1> ids{static_cast<faiss::idx_t>(id)};
  faiss::IDSelectorArray sel(ids.size(), ids.data());

  BAIDU_SCOPED_LOCK(mutex_);
  index_->remove_ids(sel);
  index_->add_with_ids(1, vector.data(), reinterpret_cast<faiss::idx_t*>(&id));
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Delete(uint64_t id) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  std::array<faiss::idx_t, 1> ids{static_cast<faiss::idx_t>(id)};
  faiss::IDSelectorArray sel(ids.size(), ids.data());
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

butil::Status VectorIndexFlat::Search(const std::vector<float>& vector, uint32_t topk,
                                      std::vector<pb::common::VectorWithDistance>& results,
                                      [[maybe_unused]] bool reconstruct) {  // NOLINT
                                                                            // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  std::vector<faiss::Index::distance_t> distances;
  distances.resize(topk, 0.0f);
  std::vector<faiss::idx_t> labels;
  labels.resize(topk, -1);

  {
    BAIDU_SCOPED_LOCK(mutex_);
    index_->search(1, vector.data(), topk, distances.data(), labels.data());
  }

  results.clear();

  for (size_t i = 0; i < topk; i++) {
    if (labels[i] < 0) {
      continue;
    }
    pb::common::VectorWithDistance vector_with_distance;
    auto* vector_with_id = vector_with_distance.mutable_vector_with_id();
    vector_with_id->set_id(labels[i]);
    vector_with_id->mutable_vector()->set_dimension(dimension_);
    vector_with_id->mutable_vector()->set_value_type(::dingodb::pb::common::ValueType::FLOAT);
    vector_with_distance.set_distance(distances[i]);

    results.emplace_back(std::move(vector_with_distance));
  }

  DINGO_LOG(DEBUG) << "result.size() = " << results.size();

  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Search(pb::common::VectorWithId vector_with_id, uint32_t topk,
                                      std::vector<pb::common::VectorWithDistance>& results,
                                      [[maybe_unused]] bool reconstruct) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  dingodb::pb::common::ValueType value_type = vector_with_id.vector().value_type();

  if (value_type != dingodb::pb::common::ValueType::FLOAT) {
    std::string s = fmt::format("Flat : {} only support float vector. not support binary vector now!",
                                static_cast<int>(value_type));
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, s);
  }

  std::vector<float> vector;
  for (const auto& value : vector_with_id.vector().float_values()) {
    vector.emplace_back(value);
  }

  // check again
  if (vector.size() != static_cast<size_t>(dimension_)) {
    std::string s =
        fmt::format("Flat : float size : {} not equal to  dimension(create) : {}", vector.size(), dimension_);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  if (0 == topk) {
    std::string s = fmt::format("Flat : topk is zero. not support");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INVALID, s);
  }

  return Search(vector, topk, results);
}

butil::Status VectorIndexFlat::SetOffline() {
  is_online_.store(false);
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::SetOnline() {
  is_online_.store(true);
  return butil::Status::OK();
}

butil::Status VectorIndexFlat::Save(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Flat index not support save");
}

butil::Status VectorIndexFlat::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Flat index not support load");
}

}  // namespace dingodb
