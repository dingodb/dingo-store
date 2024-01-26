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

#include "butil/compiler_specific.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {

// TODO: refact
void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    DINGO_LOG(FATAL) << "Encode vector key failed, prefix is 0, partition_id:[" << partition_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kVectorKeyMinLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  buf.GetBytes(result);
}

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    // Buf buf(16);
    // Buf buf(Constant::kVectorKeyMaxLen);
    // buf.WriteLong(partition_id);
    // DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);
    // buf.GetBytes(result);

    // prefix == 0 is not allowed
    DINGO_LOG(FATAL) << "Encode vector key failed, prefix is 0, partition_id:[" << partition_id << "], vector_id:["
                     << vector_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kVectorKeyMaxLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);
  buf.GetBytes(result);
}

int64_t VectorCodec::DecodeVectorId(const std::string& value) {
  Buf buf(value);
  if (value.size() == Constant::kVectorKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (value.size() == Constant::kVectorKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(FATAL) << "Decode vector id failed, value size is not 9 or 17, value:[" << Helper::StringToHex(value)
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

std::string VectorCodec::DecodeKeyToString(const std::string& key) {
  return fmt::format("{}_{}", DecodePartitionId(key), DecodeVectorId(key));
}

std::string VectorCodec::DecodeRangeToString(const pb::common::Range& range) {
  return fmt::format("[{}, {})", DecodeKeyToString(range.start_key()), DecodeKeyToString(range.end_key()));
}

void VectorCodec::DecodeRangeToVectorId(const pb::common::Range& range, int64_t& begin_vector_id,
                                        int64_t& end_vector_id) {
  begin_vector_id = VectorCodec::DecodeVectorId(range.start_key());
  int64_t temp_end_vector_id = VectorCodec::DecodeVectorId(range.end_key());
  if (temp_end_vector_id > 0) {
    end_vector_id = temp_end_vector_id;
  } else {
    if (DecodePartitionId(range.end_key()) > DecodePartitionId(range.start_key())) {
      end_vector_id = INT64_MAX;
    }
  }
}

bool VectorCodec::IsValidKey(const std::string& key) {
  // return (key.size() == 8 || key.size() == 9 || key.size() == 16 || key.size() == 17);
  return (key.size() == Constant::kVectorKeyMinLenWithPrefix || key.size() == Constant::kVectorKeyMaxLenWithPrefix);
}

bool VectorCodec::IsLegalVectorId(int64_t vector_id) { return vector_id > 0 && vector_id != INT64_MAX; }

}  // namespace dingodb