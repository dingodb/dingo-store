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

#include "vector/codec.h"

#include <cstdint>

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result) {
  if (prefix == 0) {
    // Buf buf(16);
    Buf buf(Constant::kVectorKeyMaxLen);
    buf.WriteLong(partition_id);
    DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);
    buf.GetBytes(result);
  } else {
    // Buf buf(17);
    Buf buf(Constant::kVectorKeyMaxLenWithPrefix);
    buf.WriteLong(partition_id);
    DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);
    buf.GetBytes(result);
  }
}

int64_t VectorCodec::DecodeVectorId(const std::string& value) {
  Buf buf(value);
  if (value.size() == Constant::kVectorKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (value.size() == Constant::kVectorKeyMaxLen) {
    buf.Skip(8);
  } else if (value.size() == Constant::kVectorKeyMinLen || value.size() == Constant::kVectorKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(ERROR) << "Decode vector id failed, value size is not 16 or 17, value:[" << Helper::StringToHex(value)
                     << "]";
    return 0;
  }

  // return buf.ReadLong();
  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

int64_t VectorCodec::DecodePartitionId(const std::string& value) {
  Buf buf(value);

  // if (value.size() == 17 || value.size() == 9) {
  if (value.size() == Constant::kVectorKeyMaxLenWithPrefix || value.size() == Constant::kVectorKeyMinLenWithPrefix) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

bool VectorCodec::IsValidKey(const std::string& key) {
  // return (key.size() == 8 || key.size() == 9 || key.size() == 16 || key.size() == 17);
  return (key.size() == Constant::kVectorKeyMinLen || key.size() == Constant::kVectorKeyMinLenWithPrefix ||
          key.size() == Constant::kVectorKeyMaxLen || key.size() == Constant::kVectorKeyMaxLenWithPrefix);
}

}  // namespace dingodb