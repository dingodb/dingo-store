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

#include "vector/vector_index_hnsw.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "butil/status.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

VectorIndexHnsw::VectorIndexHnsw(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
    : VectorIndex(id, vector_index_parameter) {
  bthread_mutex_init(&mutex_, nullptr);
  is_online_.store(true);

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

VectorIndexHnsw::~VectorIndexHnsw() {
  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    delete hnsw_index_;
    delete hnsw_space_;
  }

  bthread_mutex_destroy(&mutex_);
}

butil::Status VectorIndexHnsw::Add(uint64_t id, const std::vector<float>& vector) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  return Upsert(id, vector);
}

butil::Status VectorIndexHnsw::Upsert(uint64_t id, const std::vector<float>& vector) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  BAIDU_SCOPED_LOCK(mutex_);

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

butil::Status VectorIndexHnsw::Delete(uint64_t id) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

  BAIDU_SCOPED_LOCK(mutex_);

  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    try {
      hnsw_index_->markDelete(id);
    } catch (std::exception& e) {
      DINGO_LOG(ERROR) << "delete vector failed, id=" << id << ", what=" << e.what();
      return butil::Status(pb::error::Errno::EINTERNAL, std::string(e.what()));
    }
  }

  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::Save(const std::string& path) {
  if (vector_index_type == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    hnsw_index_->saveIndex(path);
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }
}

butil::Status VectorIndexHnsw::Load(const std::string& path) {
  BAIDU_SCOPED_LOCK(mutex_);

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

butil::Status VectorIndexHnsw::Search(const std::vector<float>& vector, uint32_t topk,
                                      std::vector<pb::common::VectorWithDistance>& results, bool reconstruct) {
  // check is_online
  if (!is_online_.load()) {
    std::string s = fmt::format("vector index is offline, please wait for online");
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EVECTOR_INDEX_OFFLINE, s);
  }

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

      if (reconstruct) {
        try {
          std::vector<float> data = hnsw_index_->getDataByLabel<float>(result.top().second);
          for (auto& value : data) {
            vector_with_id->mutable_vector()->add_float_values(value);
          }
          results.push_back(vector_with_distance);
        } catch (std::exception& e) {
          DINGO_LOG(ERROR) << "getDataByLabel failed, label: " << result.top().second << " err: " << e.what();
        }
      } else {
        results.push_back(vector_with_distance);
      }

      result.pop();
    }
    return butil::Status::OK();
  } else {
    return butil::Status(pb::error::Errno::EINTERNAL, "vector index type is not supported");
  }
}

butil::Status VectorIndexHnsw::SetOnline() {
  is_online_.store(true);
  return butil::Status::OK();
}

butil::Status VectorIndexHnsw::SetOffline() {
  is_online_.store(false);
  return butil::Status::OK();
}

}  // namespace dingodb
