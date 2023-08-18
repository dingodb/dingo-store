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

#ifndef DINGODB_ENGINE_STORAGE_H_
#define DINGODB_ENGINE_STORAGE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/context.h"
#include "engine/engine.h"
#include "engine/raft_store_engine.h"
#include "memory"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class Storage {
 public:
  Storage(std::shared_ptr<Engine> engine);
  ~Storage();

  static Snapshot* GetSnapshot();
  void ReleaseSnapshot();

  // kv
  butil::Status KvGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                      std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs);

  butil::Status KvPutIfAbsent(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                              bool is_atomic);

  butil::Status KvDelete(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys);

  butil::Status KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range);

  butil::Status KvCompareAndSet(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                const std::vector<std::string>& expect_values, bool is_atomic);

  butil::Status KvScanBegin(std::shared_ptr<Context> ctx, const std::string& cf_name, uint64_t region_id,
                            const pb::common::Range& range, uint64_t max_fetch_cnt, bool key_only,
                            bool disable_auto_release, bool disable_coprocessor,
                            const pb::store::Coprocessor& coprocessor, std::string* scan_id,
                            std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanContinue(std::shared_ptr<Context> ctx, const std::string& scan_id, uint64_t max_fetch_cnt,
                                      std::vector<pb::common::KeyValue>* kvs);

  static butil::Status KvScanRelease(std::shared_ptr<Context> ctx, const std::string& scan_id);

  // vector index
  butil::Status VectorAdd(std::shared_ptr<Context> ctx, const std::vector<pb::common::VectorWithId>& vectors);
  butil::Status VectorDelete(std::shared_ptr<Context> ctx, const std::vector<uint64_t>& ids);

  butil::Status VectorBatchQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                 std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorBatchSearch(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                  std::vector<pb::index::VectorWithDistanceResult>& results);
  butil::Status VectorGetBorderId(uint64_t region_id, const pb::common::Range& region_range, bool get_min,
                                  uint64_t& vector_id);
  butil::Status VectorScanQuery(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                std::vector<pb::common::VectorWithId>& vector_with_ids);
  butil::Status VectorGetRegionMetrics(uint64_t region_id, const pb::common::Range& region_range,
                                       std::shared_ptr<VectorIndex> vector_index,
                                       pb::common::VectorIndexMetrics& region_metrics);

  butil::Status VectorCalcDistance(std::shared_ptr<Context> ctx, uint64_t region_id,
                                   const ::dingodb::pb::index::VectorCalcDistanceRequest& request,
                                   std::vector<std::vector<float>>& distances,                            // NOLINT
                                   std::vector<::dingodb::pb::common::Vector>& result_op_left_vectors,    // NOLINT
                                   std::vector<::dingodb::pb::common::Vector>& result_op_right_vectors);  // NOLINT

  // This function is for testing only
  butil::Status VectorBatchSearchDebug(std::shared_ptr<Engine::VectorReader::Context> ctx,
                                       std::vector<pb::index::VectorWithDistanceResult>& results,
                                       int64_t& deserialization_id_time_us, int64_t& scan_scalar_time_us,
                                       int64_t& search_time_us);

  butil::Status ValidateLeader(uint64_t region_id);

 private:
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_STORAGE_H
