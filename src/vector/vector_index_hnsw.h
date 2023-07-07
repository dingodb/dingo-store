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

#ifndef DINGODB_VECTOR_INDEX_HNSW_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_HNSW_H_

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
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexHnsw : public VectorIndex {
 public:
  explicit VectorIndexHnsw(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
      : VectorIndex(id, vector_index_parameter) {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      const auto& hnsw_parameter = vector_index_parameter.hnsw_parameter();
      assert(hnsw_parameter.dimension() > 0);
      assert(hnsw_parameter.metric_type() != pb::common::MetricType::METRIC_TYPE_NONE);
      assert(hnsw_parameter.efconstruction() > 0);
      assert(hnsw_parameter.max_elements() > 0);
      assert(hnsw_parameter.nlinks() > 0);

      this->dimension_ = hnsw_parameter.dimension();

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

  ~VectorIndexHnsw() override {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      delete hnsw_index_;
      delete hnsw_space_;
    }
  }

  VectorIndexHnsw(const VectorIndexHnsw& rhs) = delete;
  VectorIndexHnsw& operator=(const VectorIndexHnsw& rhs) = delete;
  VectorIndexHnsw(VectorIndexHnsw&& rhs) = delete;
  VectorIndexHnsw& operator=(VectorIndexHnsw&& rhs) = delete;

  butil::Status Add(uint64_t id, const std::vector<float>& vector) override { return Upsert(id, vector); }

  butil::Status Upsert(uint64_t id, const std::vector<float>& vector) override {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      if (vector.size() != this->dimension_) {
        DINGO_LOG(ERROR) << "vector dimension is not match, id=" << id << ", input dimension=" << vector.size() << ", "
                         << "index dimension=" << this->dimension_;
        return butil::Status(pb::error::Errno::EINTERNAL, "vector dimension is not match");
      }

      hnsw_index_->addPoint(vector.data(), id, true);

      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  void Delete(uint64_t id) override {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      try {
        hnsw_index_->markDelete(id);
      } catch (std::exception& e) {
        DINGO_LOG(ERROR) << "delete vector failed, id=" << id << ", what=" << e.what();
      }
    }
  }

  butil::Status Save(const std::string& path) override {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->saveIndex(path);
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  butil::Status Load(const std::string& path) override {
    // FIXME: need to prevent SEGV when delete old_hnsw_index
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      auto* old_hnsw_index = hnsw_index_;
      hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, path, false,
                                                        vector_index_parameter.hnsw_parameter().max_elements(), true);
      delete old_hnsw_index;
      return butil::Status::OK();
    } else {
      return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
    }
  }

  butil::Status Search(const std::vector<float>& vector, uint32_t topk,
                       std::vector<pb::common::VectorWithDistance>& results) override {
    if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      // std::priority_queue<std::pair<float, uint64_t>> result = hnsw_index_->searchKnn(vector.data(), topk);

      if (vector.size() != this->dimension_) {
        return butil::Status(pb::error::Errno::EINTERNAL, "vector dimension is not match, input=%zu, index=%d",
                             vector.size(), this->dimension_);
      }

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
                       std::vector<pb::common::VectorWithDistance>& results) override {
    std::vector<float> vector;
    for (auto value : vector_with_id.vector().float_values()) {
      vector.push_back(value);
    }

    return Search(vector, topk, results);
  }

 private:
  // hnsw members
  hnswlib::HierarchicalNSW<float>* hnsw_index_;
  hnswlib::SpaceInterface<float>* hnsw_space_;

  // Dimension of the elements
  uint32_t dimension_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_HNSW_H_
