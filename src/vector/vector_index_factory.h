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

#ifndef DINGODB_VECTOR_INDEX_FACTORY_H_
#define DINGODB_VECTOR_INDEX_FACTORY_H_

#include <cstdint>
#include <memory>

#include "common/threadpool.h"
#include "proto/common.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexFactory {
 public:
  VectorIndexFactory() = delete;
  ~VectorIndexFactory() = delete;

  VectorIndexFactory(const VectorIndexFactory& rhs) = delete;
  VectorIndexFactory& operator=(const VectorIndexFactory& rhs) = delete;
  VectorIndexFactory(VectorIndexFactory&& rhs) = delete;
  VectorIndexFactory& operator=(VectorIndexFactory&& rhs) = delete;

  static std::shared_ptr<VectorIndex> New(int64_t id, const pb::common::VectorIndexParameter& index_parameter,
                                          const pb::common::RegionEpoch& epoch, const pb::common::Range& range);

  static std::shared_ptr<VectorIndex> NewHnsw(int64_t id, const pb::common::VectorIndexParameter& index_parameter,
                                              const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                              ThreadPoolPtr thread_pool);

  static std::shared_ptr<VectorIndex> NewFlat(int64_t id, const pb::common::VectorIndexParameter& index_parameter,
                                              const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                              ThreadPoolPtr thread_pool);

  static std::shared_ptr<VectorIndex> NewIvfFlat(int64_t id, const pb::common::VectorIndexParameter& index_parameter,
                                                 const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                                 ThreadPoolPtr thread_pool);

  static std::shared_ptr<VectorIndex> NewIvfPq(int64_t id, const pb::common::VectorIndexParameter& index_parameter,
                                               const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                                               ThreadPoolPtr thread_pool);

  static std::shared_ptr<VectorIndex> NewBruteForce(int64_t id, const pb::common::VectorIndexParameter& index_parameter,
                                                    const pb::common::RegionEpoch& epoch,
                                                    const pb::common::Range& range, ThreadPoolPtr thread_pool);
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_FACTORY_H_
