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

#ifndef DINGODB_VECTOR_CODEC_H_
#define DINGODB_VECTOR_CODEC_H_

#include <cstdint>
#include <string>

#include "proto/common.pb.h"

namespace dingodb {

class VectorCodec {
 public:
  // Foramt: prefix/partition_id/vector_id
  static void EncodeVectorData(uint64_t partition_id, uint64_t vector_id, std::string& result);
  static void EncodeVectorScalar(uint64_t partition_id, uint64_t vector_id, std::string& result);
  static void EncodeVectorTable(uint64_t partition_id, uint64_t vector_id, std::string& result);

  static uint64_t DecodeVectorId(const std::string& value);
  static uint64_t DecodePartitionId(const std::string& value);

  static std::string FillVectorDataPrefix(const std::string& value);
  static std::string FillVectorScalarPrefix(const std::string& value);
  static std::string FillVectorTablePrefix(const std::string& value);

  static std::string EncodeVectorIndexLogIndex(uint64_t snapshot_log_index, uint64_t apply_log_index);
  static int DecodeVectorIndexLogIndex(const std::string& value, uint64_t& snapshot_log_index,
                                       uint64_t& apply_log_index);
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_CODEC_H_