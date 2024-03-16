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

#include <cstdint>
#include <memory>

#include "common/logging.h"
#include "proto/common.pb.h"
#include "server/server.h"
#include "vector/vector_index.h"
#include "vector/vector_index_bruteforce.h"
#include "vector/vector_index_flat.h"
#include "vector/vector_index_hnsw.h"
#include "vector/vector_index_ivf_flat.h"
#include "vector/vector_index_ivf_pq.h"

namespace dingodb {

std::shared_ptr<VectorIndex> VectorIndexFactory::New(int64_t id,
                                                     const pb::common::VectorIndexParameter& index_parameter,
                                                     const pb::common::RegionEpoch& epoch,
                                                     const pb::common::Range& range) {
  std::shared_ptr<VectorIndex> vector_index = nullptr;

  auto thread_pool = Server::GetInstance().GetVectorIndexThreadPool();

  switch (index_parameter.vector_index_type()) {
    case pb::common::VECTOR_INDEX_TYPE_BRUTEFORCE: {
      vector_index = NewBruteForce(id, index_parameter, epoch, range, thread_pool);
      break;
    }
    case pb::common::VECTOR_INDEX_TYPE_FLAT: {
      vector_index = NewFlat(id, index_parameter, epoch, range, thread_pool);
      break;
    }
    case pb::common::VECTOR_INDEX_TYPE_IVF_FLAT: {
      vector_index = NewIvfFlat(id, index_parameter, epoch, range, thread_pool);
      break;
    }
    case pb::common::VECTOR_INDEX_TYPE_IVF_PQ: {
      vector_index = NewIvfPq(id, index_parameter, epoch, range, thread_pool);
      break;
    }
    case pb::common::VECTOR_INDEX_TYPE_HNSW: {
      vector_index = NewHnsw(id, index_parameter, epoch, range, thread_pool);
      break;
    }
    case pb::common::VECTOR_INDEX_TYPE_DISKANN: {
      DINGO_LOG(ERROR) << "vector_index_parameter = diskann not implement, type=" << index_parameter.vector_index_type()
                       << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
      break;
    }
    case pb::common::VectorIndexType_INT_MIN_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    case pb::common::VectorIndexType_INT_MAX_SENTINEL_DO_NOT_USE_:
      [[fallthrough]];
    case pb::common::VECTOR_INDEX_TYPE_NONE:
      [[fallthrough]];
    default: {
      DINGO_LOG(ERROR) << "vector_index_parameter is invalid, type=" << index_parameter.vector_index_type()
                       << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
      break;
    }
  }

  return vector_index;
}

std::shared_ptr<VectorIndex> VectorIndexFactory::NewHnsw(int64_t id,
                                                         const pb::common::VectorIndexParameter& index_parameter,
                                                         const pb::common::RegionEpoch& epoch,
                                                         const pb::common::Range& range, ThreadPoolPtr thread_pool) {
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
    auto new_hnsw_index = std::make_shared<VectorIndexHnsw>(id, index_parameter, epoch, range, thread_pool);
    if (new_hnsw_index == nullptr) {
      DINGO_LOG(ERROR) << "create hnsw index failed of new_hnsw_index is nullptr, id=" << id
                       << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    } else {
      DINGO_LOG(INFO) << "create hnsw index success, id=" << id << ", parameter=" << index_parameter.ShortDebugString();
    }
    return new_hnsw_index;
  } catch (std::exception& e) {
    DINGO_LOG(ERROR) << "create hnsw index failed of exception occured, " << e.what() << ", id=" << id
                     << ", parameter=" << index_parameter.ShortDebugString();
    return nullptr;
  }
}

std::shared_ptr<VectorIndex> VectorIndexFactory::NewBruteForce(int64_t id,
                                                               const pb::common::VectorIndexParameter& index_parameter,
                                                               const pb::common::RegionEpoch& epoch,
                                                               const pb::common::Range& range,
                                                               ThreadPoolPtr thread_pool) {
  const auto& bruteforce_parameter = index_parameter.bruteforce_parameter();

  if (bruteforce_parameter.dimension() <= 0) {
    DINGO_LOG(ERROR) << "vector_index_parameter is illegal, dimension <= 0";
    return nullptr;
  }
  if (bruteforce_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_NONE) {
    DINGO_LOG(ERROR) << "vector_index_parameter is illegal, METRIC_TYPE_NONE";
    return nullptr;
  }

  // create index may throw exeception, so we need to catch it
  try {
    auto new_bruteforce_index = std::make_shared<VectorIndexBruteforce>(id, index_parameter, epoch, range, thread_pool);
    if (new_bruteforce_index == nullptr) {
      DINGO_LOG(ERROR) << "create bruteforce index failed of new_bruteforce_index is nullptr"
                       << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    } else {
      DINGO_LOG(INFO) << "create bruteforce index success, id=" << id
                      << ", parameter=" << index_parameter.ShortDebugString();
    }
    return new_bruteforce_index;
  } catch (std::exception& e) {
    DINGO_LOG(ERROR) << "create bruteforce index failed of exception occured, " << e.what() << ", id=" << id
                     << ", parameter=" << index_parameter.ShortDebugString();
    return nullptr;
  }
}

std::shared_ptr<VectorIndex> VectorIndexFactory::NewFlat(int64_t id,
                                                         const pb::common::VectorIndexParameter& index_parameter,
                                                         const pb::common::RegionEpoch& epoch,
                                                         const pb::common::Range& range, ThreadPoolPtr thread_pool) {
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
    auto new_flat_index = std::make_shared<VectorIndexFlat>(id, index_parameter, epoch, range, thread_pool);
    if (new_flat_index == nullptr) {
      DINGO_LOG(ERROR) << "create flat index failed of new_flat_index is nullptr"
                       << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    } else {
      DINGO_LOG(INFO) << "create flat index success, id=" << id << ", parameter=" << index_parameter.ShortDebugString();
    }
    return new_flat_index;
  } catch (std::exception& e) {
    DINGO_LOG(ERROR) << "create flat index failed of exception occured, " << e.what() << ", id=" << id
                     << ", parameter=" << index_parameter.ShortDebugString();
    return nullptr;
  }
}

std::shared_ptr<VectorIndex> VectorIndexFactory::NewIvfFlat(int64_t id,
                                                            const pb::common::VectorIndexParameter& index_parameter,
                                                            const pb::common::RegionEpoch& epoch,
                                                            const pb::common::Range& range, ThreadPoolPtr thread_pool) {
  const auto& ivf_flat_parameter = index_parameter.ivf_flat_parameter();

  if (ivf_flat_parameter.dimension() <= 0) {
    DINGO_LOG(ERROR) << "vector_index_parameter is illegal, dimension <= 0 : " << ivf_flat_parameter.dimension();
    return nullptr;
  }
  if (ivf_flat_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_NONE) {
    DINGO_LOG(ERROR) << "vector_index_parameter is illegal, METRIC_TYPE_NONE";
    return nullptr;
  }

  // if <=0 use default

  // if (ivf_flat_parameter.ncentroids() <= 0) {
  //   DINGO_LOG(ERROR) << "vector_index_parameter is illegal, ncentroids <=0 : " << ivf_flat_parameter.ncentroids();
  //   return nullptr;
  // }

  // create index may throw exception, so we need to catch it
  try {
    auto new_ivf_flat_index = std::make_shared<VectorIndexIvfFlat>(id, index_parameter, epoch, range, thread_pool);
    if (new_ivf_flat_index == nullptr) {
      DINGO_LOG(ERROR) << "create ivf flat index failed of new_ivf_flat_index is nullptr"
                       << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    } else {
      DINGO_LOG(INFO) << "create ivf flat index success, id=" << id
                      << ", parameter=" << index_parameter.ShortDebugString();
    }
    return new_ivf_flat_index;
  } catch (std::exception& e) {
    DINGO_LOG(ERROR) << "create ivf flat index failed of exception occurred, " << e.what() << ", id=" << id
                     << ", parameter=" << index_parameter.ShortDebugString();
    return nullptr;
  }
}

std::shared_ptr<VectorIndex> VectorIndexFactory::NewIvfPq(int64_t id,
                                                          const pb::common::VectorIndexParameter& index_parameter,
                                                          const pb::common::RegionEpoch& epoch,
                                                          const pb::common::Range& range, ThreadPoolPtr thread_pool) {
  const auto& ivf_pq_parameter = index_parameter.ivf_pq_parameter();

  uint32_t dimension = ivf_pq_parameter.dimension();
  if (dimension <= 0) {
    DINGO_LOG(ERROR) << fmt::format("vector_index_parameter is illegal, dimension : {} <= 0 : ",
                                    ivf_pq_parameter.dimension());
    return nullptr;
  }
  if (ivf_pq_parameter.metric_type() == pb::common::MetricType::METRIC_TYPE_NONE) {
    DINGO_LOG(ERROR) << "vector_index_parameter is illegal, METRIC_TYPE_NONE";
    return nullptr;
  }

  int32_t ncentroids = ivf_pq_parameter.ncentroids();
  if (ncentroids < 0) {
    DINGO_LOG(ERROR) << fmt::format("vector_index_parameter is illegal, ncentroids:{} must >= 0 ",
                                    ivf_pq_parameter.ncentroids());
    return nullptr;
  }

  if (0 == ncentroids) {
    ncentroids = Constant::kCreateIvfPqParamNcentroids;
    DINGO_LOG(INFO) << "vector_index_parameter ncentroids = 0, use default " << Constant::kCreateIvfPqParamNcentroids;
  }

  int32_t nsubvector = ivf_pq_parameter.nsubvector();
  if (nsubvector < 0) {
    DINGO_LOG(ERROR) << fmt::format("vector_index_parameter is illegal, nsubvector : {} must >= 0 ",
                                    ivf_pq_parameter.nsubvector());
    return nullptr;
  }

  if (0 == nsubvector) {
    nsubvector = Constant::kCreateIvfPqParamNsubvector;
    DINGO_LOG(INFO) << "vector_index_parameter nsubvector = 0, use default " << Constant::kCreateIvfPqParamNsubvector;
  }

  int32_t nbits_per_idx = ivf_pq_parameter.nbits_per_idx();
  if (nbits_per_idx < 0) {
    DINGO_LOG(ERROR) << fmt::format("vector_index_parameter is illegal, nbits_per_idx : {} must >= 0 ",
                                    ivf_pq_parameter.nbits_per_idx());
    return nullptr;
  }

  nbits_per_idx %= 64;

  if (0 == nbits_per_idx) {
    nbits_per_idx = Constant::kCreateIvfPqParamNbitsPerIdx;
    DINGO_LOG(INFO) << "vector_index_parameter nsubvector = 0, use default " << Constant::kCreateIvfPqParamNbitsPerIdx;
  }

  if (0 != (dimension % nsubvector)) {
    DINGO_LOG(ERROR) << fmt::format("vector_index_parameter is illegal, dimension:{} / nsubvector:{} not divisible ",
                                    dimension, nsubvector);
    return nullptr;
  }

  if (nbits_per_idx > Constant::kCreateIvfPqParamNbitsPerIdxMaxWarning) {
    DINGO_LOG(WARNING) << fmt::format(
        "vector_index_parameter nbits_per_idx : {} > kCreateIvfPqParamNbitsPerIdxMaxWarning : {}. may be not alloc "
        "enough memory.",
        nbits_per_idx, Constant::kCreateIvfPqParamNbitsPerIdxMaxWarning);
  }

  // create index may throw exception, so we need to catch it
  try {
    auto new_ivf_pq_index = std::make_shared<VectorIndexIvfPq>(id, index_parameter, epoch, range, thread_pool);
    if (new_ivf_pq_index == nullptr) {
      DINGO_LOG(ERROR) << "create ivf pq index failed of new_ivf_pq_index is nullptr"
                       << ", id=" << id << ", parameter=" << index_parameter.ShortDebugString();
      return nullptr;
    } else {
      DINGO_LOG(INFO) << "create ivf pq index success, id=" << id
                      << ", parameter=" << index_parameter.ShortDebugString();
    }
    return new_ivf_pq_index;
  } catch (std::exception& e) {
    DINGO_LOG(ERROR) << "create ivf pq index failed of exception occurred, " << e.what() << ", id=" << id
                     << ", parameter=" << index_parameter.ShortDebugString();
    return nullptr;
  }
}

}  // namespace dingodb
