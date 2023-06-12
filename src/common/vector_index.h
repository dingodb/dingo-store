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

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "brpc/controller.h"
#include "brpc/server.h"
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "common/logging.h"
#include "hnswlib/hnswlib.h"
#include "proto/common.pb.h"

namespace dingodb {

class VectorIndex {
 public:
  VectorIndex(uint32_t dimension, uint32_t max_elements)
      : vector_index_type_(pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW),
        dimension_(dimension),
        max_elements_(max_elements) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      int m = 16;                 // Tightly connected with internal dimensionality of the data
                                  // strongly affects the memory consumption
      int ef_construction = 200;  // Controls index search speed/build speed tradeoff

      // Initing index
      hnsw_space_ = new hnswlib::L2Space(dimension_);
      hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, max_elements_, m, ef_construction);
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

  pb::common::VectorIndexType VectorIndexType() { return vector_index_type_; }

  void Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
    for (const auto& vector_with_id : vector_with_ids) {
      Add(vector_with_id);
    }
  }

  void Add(const pb::common::VectorWithId& vector_with_id) {
    std::vector<float> vector;
    for (const auto& value : vector_with_id.vector().values()) {
      vector.push_back(value);
    }

    Add(vector_with_id.id(), vector);
  }

  void Add(uint64_t id, const std::vector<float>& vector) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->addPoint(vector.data(), id);
    }
  }

  void Delete(uint64_t id) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->markDelete(id);
    }
  }

  void Save(const std::string& path) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      hnsw_index_->saveIndex(path);
    }
  }

  void Load(const std::string& path) {
    // FIXME: need to prevent SEGV when delete old_hnsw_index
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      auto* old_hnsw_index = hnsw_index_;
      hnsw_index_ = new hnswlib::HierarchicalNSW<float>(hnsw_space_, path);
      delete old_hnsw_index;
    }
  }

  void Search(const std::vector<float>& vector, uint32_t topk, std::vector<pb::common::VectorWithDistance>& results) {
    if (vector_index_type_ == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
      // std::priority_queue<std::pair<float, uint64_t>> result = hnsw_index_->searchKnn(vector.data(), topk);
      std::priority_queue<std::pair<float, uint64_t>> result = hnsw_index_->searchKnn(vector.data(), topk);

      DINGO_LOG(DEBUG) << "result.size() = " << result.size();

      while (!result.empty()) {
        pb::common::VectorWithDistance vector_with_distance;
        vector_with_distance.set_id(result.top().second);
        vector_with_distance.set_distance(result.top().first);

        std::vector<float> data = hnsw_index_->getDataByLabel<float>(result.top().second);
        for (auto& value : data) {
          vector_with_distance.mutable_vector()->add_values(value);
        }

        results.push_back(vector_with_distance);

        result.pop();
      }
    }
  }

  void Search(pb::common::VectorWithId vector_with_id, uint32_t topk,
              std::vector<pb::common::VectorWithDistance>& results) {
    std::vector<float> vector;
    for (auto value : vector_with_id.vector().values()) {
      vector.push_back(value);
    }

    Search(vector, topk, results);
  }

 private:
  pb::common::VectorIndexType vector_index_type_;
  hnswlib::HierarchicalNSW<float>* hnsw_index_;
  hnswlib::L2Space* hnsw_space_;
  uint32_t dimension_;     // Dimension of the elements
  uint32_t max_elements_;  // Maximum number of elements, should be known beforehand
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_
