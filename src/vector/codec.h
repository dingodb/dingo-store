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
  static void EncodeVectorKey(char prefix, int64_t partition_id, std::string& result);
  static void EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result);

  static int64_t DecodeVectorId(const std::string& value);
  static int64_t DecodePartitionId(const std::string& value);

  static std::string DecodeKeyToString(const std::string& key);
  static std::string DecodeRangeToString(const pb::common::Range& range);

  static void DecodeRangeToVectorId(const pb::common::Range& range, int64_t& begin_vector_id, int64_t& end_vector_id);

  static bool IsValidKey(const std::string& key);

  static bool IsLegalVectorId(int64_t vector_id);
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_CODEC_H_