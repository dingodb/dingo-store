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

#ifndef DINGODB_SDK_VECTOR_INDEX_CREATOR_DATA_H_
#define DINGODB_SDK_VECTOR_INDEX_CREATOR_DATA_H_

#include <optional>
#include <utility>

#include "proto/common.pb.h"
#include "sdk/client_stub.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

class VectorIndexCreator::Data {
 public:
  Data(const Data&) = delete;
  const Data& operator=(const Data&) = delete;

  explicit Data(const ClientStub& stub)
      : stub(stub),
        schema_id(-1),
        version(1),
        replica_num(3),
        index_type(kNoneIndexType),
        auto_incr(false),
        wait(true) {}

  ~Data() = default;

  void BuildIndexParameter(pb::common::IndexParameter* index_parameter) {
    index_parameter->set_index_type(pb::common::IndexType::INDEX_TYPE_VECTOR);
    auto* parameter = index_parameter->mutable_vector_index_parameter();
    if (index_type == kFlat) {
      DCHECK(flat_param.has_value());
      auto& param = flat_param.value();
      FillFlatParmeter(parameter, param);
    } else if (index_type == kIvfFlat) {
      DCHECK(ivf_flat_param.has_value());
      auto& param = ivf_flat_param.value();
      FillIvfFlatParmeter(parameter, param);
    } else if (index_type == kIvfPq) {
      DCHECK(ivf_pq_param.has_value());
      auto& param = ivf_pq_param.value();
      FillIvfPqParmeter(parameter, param);
    } else if (index_type == kHnsw) {
      DCHECK(hnsw_param.has_value());
      auto& param = hnsw_param.value();
      FillHnswParmeter(parameter, param);
    } else if (index_type == kBruteForce) {
      DCHECK(brute_force_param.has_value());
      auto& param = brute_force_param.value();
      FillButeForceParmeter(parameter, param);
    } else {
      CHECK(false) << "unsupported index type, " << index_type;
    }
  }

  const ClientStub& stub;
  int64_t schema_id;
  std::string index_name;
  // TODO: support version
  int32_t version;
  std::vector<int64_t> range_partition_seperator_ids;
  int64_t replica_num;

  VectorIndexType index_type;
  std::optional<FlatParam> flat_param;
  std::optional<IvfFlatParam> ivf_flat_param;
  std::optional<IvfPqParam> ivf_pq_param;
  std::optional<HnswParam> hnsw_param;
  std::optional<DiskAnnParam> diskann_param;
  std::optional<BruteForceParam> brute_force_param;

  // TODO: Support
  bool auto_incr;
  std::optional<int64_t> auto_incr_start;
  bool wait;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_INDEX_CREATOR_DATA_H_