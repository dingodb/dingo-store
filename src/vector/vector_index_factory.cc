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

#include "vector/vector_index_factory.h"

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
#include "server/server.h"
#include "vector/vector_index.h"
#include "vector/vector_index_flat.h"
#include "vector/vector_index_hnsw.h"

namespace dingodb {

std::shared_ptr<VectorIndex> VectorIndexFactory::New(uint64_t id,
                                                     const pb::common::VectorIndexParameter& index_parameter,
                                                     const pb::common::Range& range) {
  if (index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_HNSW) {
    const auto& hnsw_parameter = index_parameter.hnsw_parameter();

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

    // create index may throw exeception, so we need to catch it
    try {
      auto new_hnsw_index = std::make_shared<VectorIndexHnsw>(id, index_parameter, range);
      if (new_hnsw_index == nullptr) {
        DINGO_LOG(ERROR) << "create hnsw index failed of new_hnsw_index is nullptr, id=" << id
                         << ", parameter=" << index_parameter.ShortDebugString();
        return nullptr;
      } else {
        DINGO_LOG(INFO) << "create hnsw index success, id=" << id
                        << ", parameter=" << index_parameter.ShortDebugString();
      }
      return new_hnsw_index;
    } catch (std::exception& e) {
      DINGO_LOG(ERROR) << "create hnsw index failed of exception occured, " << e.what() << ", id=" << id
                       << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    }
  } else if (index_parameter.vector_index_type() == pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT) {
    const auto& flat_parameter = index_parameter.flat_parameter();

    if (flat_parameter.dimension() <= 0) {
      DINGO_LOG(ERROR) << "vector_index_parameter is illegal, dimension <= 0";
      return nullptr;
    }
    if (flat_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_NONE) {
      DINGO_LOG(ERROR) << "vector_index_parameter is illegal, METRIC_TYPE_NONE";
      return nullptr;
    }

    // create index may throw exeception, so we need to catch it
    try {
      auto new_flat_index = std::make_shared<VectorIndexFlat>(id, index_parameter, range);
      if (new_flat_index == nullptr) {
        DINGO_LOG(ERROR) << "create flat index failed of new_flat_index is nullptr"
                         << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
        return nullptr;
      } else {
        DINGO_LOG(INFO) << "create flat index success, id=" << id
                        << ", parameter=" << index_parameter.ShortDebugString();
      }
      return new_flat_index;
    } catch (std::exception& e) {
      DINGO_LOG(ERROR) << "create flat index failed of exception occured, " << e.what() << ", id=" << id
                       << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    }
  } else {
    DINGO_LOG(ERROR) << "vector_index_parameter is not hnsw index or flat, type=" << index_parameter.vector_index_type()
                     << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
    return nullptr;
  }

  DINGO_LOG(ERROR) << "create vector index failed of unknown error"
                   << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
  return nullptr;
}

}  // namespace dingodb
