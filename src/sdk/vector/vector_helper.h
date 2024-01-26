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

#ifndef DINGODB_SDK_VECTOR_HELPER_H_
#define DINGODB_SDK_VECTOR_HELPER_H_

#include "glog/logging.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index.h"
#include "vector/codec.h"

namespace dingodb {
namespace sdk {
namespace vector_helper {
static std::string VectorIdToRangeKey(const VectorIndex& vector_index, int64_t vector_id) {
  int64_t part_id = vector_index.GetPartitionId(vector_id);
  CHECK_GT(part_id, 0);
  CHECK_GT(vector_id, 0);

  std::string tmp_key;
  VectorCodec::EncodeVectorKey(kVectorPrefix, part_id, vector_id, tmp_key);
  return std::move(tmp_key);
}
}  // namespace vector_helper

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_HELPER_H_