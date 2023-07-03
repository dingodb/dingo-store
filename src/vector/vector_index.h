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

#ifndef DINGODB_VECTOR_INDEX_H_
#define DINGODB_VECTOR_INDEX_H_

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class VectorIndex {
 public:
  VectorIndex(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
      : id_(id), apply_log_index_(0), snapshot_log_index_(0), vector_index_parameter_(vector_index_parameter) {
    vector_index_type_ = vector_index_parameter_.vector_index_type();

    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      const auto& hnsw_parameter = vector_index_parameter_.hnsw_parameter();
      assert(hnsw_parameter.dimension() > 0);
      assert(hnsw_parameter.metric_type() != pb::common::MetricType::METRIC_TYPE_NONE);
      assert(hnsw_parameter.efconstruction() > 0);
      assert(hnsw_parameter.max_elements() > 0);
      assert(hnsw_parameter.nlinks() > 0);

      if (hnsw_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_INNER_PRODUCT) {
        hnsw_space_ = new hnswlib::InnerProductSpace(hnsw_parameter.dimension());
      } else {
        hnsw_space_ = new hnswlib::L2Space(hnsw_parameter.dimension());
      }

      hnsw_index_ =
          new hnswlib::HierarchicalNSW<float>(hnsw_space_, hnsw_parameter.max_elements(), hnsw_parameter.nlinks(),
                                              hnsw_parameter.efconstruction(), 100, true);
    } else {
      hnsw_index_ = nullptr;
    }
  }

  ~VectorIndex() {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      delete hnsw_index_;
      delete hnsw_space_;
    }
  }

  static std::shared_ptr<VectorIndex> New(uint64_t id, const pb::common::IndexParameter& index_parameter) {
    if (index_parameter.index_type() != pb::common::IndexType::INDEX_TYPE_VECTOR) {
      DINGO_LOG(ERROR) << "index_parameter is not vector index, type=" << index_parameter.index_type();
      return nullptr;
    }

    const auto& vector_index_parameter = index_parameter.vector_index_parameter();
    if (vector_index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();

      if (hnsw_parameter.dimension() == 0) {
        DINGO_LOG(ERROR) << "vector_index_parameter is illegal, dimension is 0";
        return nullptr;
      }
      if (hnsw_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_NONE) {
        DINGO_LOG(ERROR) << "vector_index_parameter is illegal, ef_construction is 0";
        return nullptr;
      }
      if (hnsw_parameter.efconstruction() == 0) {
        DINGO_LOG(ERROR) << "vector_index_parameter is illegal, efconstruction is 0";
        return nullptr;
      }
      if (hnsw_parameter.max_elements() == 0) {
        DINGO_LOG(ERROR) << "vector_index_parameter is illegal, max_elements is 0";
        return nullptr;
      }
      if (hnsw_parameter.nlinks() == 0) {
        DINGO_LOG(ERROR) << "vector_index_parameter is illegal, nlinks is 0";
        return nullptr;
      }

      return std::make_shared<VectorIndex>(id, vector_index_parameter);
    } else {
      DINGO_LOG(ERROR) << "vector_index_parameter is not hnsw index, type="
                       << vector_index_parameter.vector_index_type();
      return nullptr;
    }
  }

  pb::common::VectorIndexType VectorIndexType() { return vector_index_type_; }

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
    for (const auto& vector_with_id : vector_with_ids) {
      auto ret = Add(vector_with_id);
      if (!ret.ok()) {
        return ret;
      }
    }
    return butil::Status::OK();
  }

  butil::Status Add(const pb::common::VectorWithId& vector_with_id) {
    std::vector<float> vector;
    for (const auto& value : vector_with_id.vector().float_values()) {
      vector.push_back(value);
    }

    return Add(vector_with_id.id(), vector);
  }

  butil::Status Add(uint64_t id, const std::vector<float>& vector) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->addPoint(vector.data(), id, true);
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  void Delete(uint64_t id) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      try {
        hnsw_index_->markDelete(id);
      } catch (std::exception& e) {
        DINGO_LOG(ERROR) << "delete vector failed, id=" << id << ", what=" << e.what();
      }
    }
  }

  butil::Status Save(const std::string& path) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->saveIndex(path);
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  butil::Status Load(const std::string& path) {
    // FIXME: need to prevent SEGV when delete old_hnsw_index
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      auto* old_hnsw_index = hnsw_index_;
      hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, path, false,
                                                        vector_index_parameter_.hnsw_parameter().max_elements(), true);
      delete old_hnsw_index;
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  butil::Status Search(const std::vector<float>& vector, uint32_t topk,
                       std::vector<pb::common::VectorWithDistance>& results) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      // std::priority_queue<std::pair<float, uint64_t>> result = hnsw_index_->searchKnn(vector.data(), topk);
      std::priority_queue<std::pair<float, uint64_t>> result = hnsw_index_->searchKnn(vector.data(), topk);

      DINGO_LOG(DEBUG) << "result.size() = " << result.size();

      while (!result.empty()) {
        pb::common::VectorWithDistance vector_with_distance;
        vector_with_distance.set_distance(result.top().first);

        auto* vector_with_id = vector_with_distance.mutable_vector_with_id();

        vector_with_id->set_id(result.top().second);

        try {
          std::vector<float> data = hnsw_index_->getDataByLabel<float>(result.top().second);
          for (auto& value : data) {
            vector_with_id->mutable_vector()->add_float_values(value);
          }
          results.push_back(vector_with_distance);
        } catch (std::exception& e) {
          DINGO_LOG(ERROR) << "getDataByLabel failed, label: " << result.top().second << " err: " << e.what();
        }

        result.pop();
      }
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  butil::Status Search(pb::common::VectorWithId vector_with_id, uint32_t topk,
                       std::vector<pb::common::VectorWithDistance>& results) {
    std::vector<float> vector;
    for (auto value : vector_with_id.vector().float_values()) {
      vector.push_back(value);
    }

    return Search(vector, topk, results);
  }

  uint64_t Id() const { return id_; }

  uint64_t ApplyLogIndex() const { return apply_log_index_.load(std::memory_order_relaxed); }

  void SetApplyLogIndex(uint64_t apply_log_index) {
    apply_log_index_.store(apply_log_index, std::memory_order_relaxed);
  }

  uint64_t SnapshotLogIndex() const { return snapshot_log_index_.load(std::memory_order_relaxed); }

  void SetSnapshotLogIndex(uint64_t snapshot_log_index) {
    snapshot_log_index_.store(snapshot_log_index, std::memory_order_relaxed);
  }

 private:
  // region_id
  uint64_t id_;
  // apply max log index
  std::atomic<uint64_t> apply_log_index_;
  // last snapshot log index
  std::atomic<uint64_t> snapshot_log_index_;

  pb::common::VectorIndexType vector_index_type_;

  // hnsw members
  hnswlib::HierarchicalNSW<float>* hnsw_index_;
  hnswlib::SpaceInterface<float>* hnsw_space_;

  uint32_t dimension_;  // Dimension of the elements
  pb::common::VectorIndexParameter vector_index_parameter_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_
