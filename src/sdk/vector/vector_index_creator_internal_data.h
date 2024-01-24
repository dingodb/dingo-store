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

  void BuildVectorIndexParameter(pb::common::VectorIndexParameter* parameter) {
    if (index_type == kFlat) {
      DCHECK(flat_param.has_value());
      parameter->set_vector_index_type(pb::common::VectorIndexType::VECTOR_INDEX_TYPE_FLAT);
      auto* flat = parameter->mutable_flat_parameter();

      auto& param = flat_param.value();
      flat->set_dimension(param.dimension);
      flat->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
    } else if (index_type == kIvfFlat) {
      DCHECK(ivf_flat_param.has_value());
      parameter->set_vector_index_type(pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_FLAT);
      auto* ivf_flat = parameter->mutable_ivf_flat_parameter();

      auto& param = ivf_flat_param.value();
      ivf_flat->set_dimension(param.dimension);
      ivf_flat->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
      ivf_flat->set_ncentroids(param.ncentroids);
    } else if (index_type == kIvfPq) {
      DCHECK(ivf_pq_param.has_value());
      parameter->set_vector_index_type(pb::common::VectorIndexType::VECTOR_INDEX_TYPE_IVF_PQ);
      auto* ivf_pq = parameter->mutable_ivf_pq_parameter();

      auto& param = ivf_pq_param.value();
      ivf_pq->set_dimension(param.dimension);
      ivf_pq->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
      ivf_pq->set_ncentroids(param.ncentroids);
      ivf_pq->set_nsubvector(param.nsubvector);
      ivf_pq->set_nbits_per_idx(param.nbits_per_idx);
    } else if (index_type == kHnsw) {
      DCHECK(hnsw_param.has_value());
      auto* hsnw = parameter->mutable_hnsw_parameter();

      auto& param = hnsw_param.value();
      hsnw->set_dimension(param.dimension);
      hsnw->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
      hsnw->set_efconstruction(param.ef_construction);
      hsnw->set_nlinks(param.nlinks);
      hsnw->set_max_elements(param.max_elements);
    } else if (index_type == kBruteForce) {
      DCHECK(brute_force_param.has_value());
      auto* hsnw = parameter->mutable_hnsw_parameter();

      auto& param = brute_force_param.value();
      auto* bruteforce = parameter->mutable_bruteforce_parameter();
      bruteforce->set_dimension(param.dimension);
      bruteforce->set_metric_type(MetricType2InternalMetricTypePB(param.metric_type));
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