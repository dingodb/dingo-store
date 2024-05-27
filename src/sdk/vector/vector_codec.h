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

#ifndef DINGODB_SDK_VECTOR_CODEC_H_
#define DINGODB_SDK_VECTOR_CODEC_H_

#include <optional>

#include "common/constant.h"
#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/utils/codec.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {
namespace sdk {

static const char kVectorPrefix = 'r';

namespace vector_codec {
static void EncodeVectorKey(char prefix, int64_t partition_id, std::string& result) {
  CHECK(prefix != 0) << "Encode vector key failed, prefix is 0, partition_id:[" << partition_id << "]";

  // Buf buf(17);
  Buf buf(Constant::kVectorKeyMinLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  buf.GetBytes(result);
}

static void EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result) {
  CHECK(prefix != 0) << "Encode vector key failed, prefix is 0, partition_id:[" << partition_id << "], vector_id:["
                     << vector_id << "]";

  // Buf buf(17);
  Buf buf(Constant::kVectorKeyMaxLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);
  buf.GetBytes(result);
}

static int64_t DecodeVectorId(const std::string& value) {
  Buf buf(value);
  if (value.size() >= Constant::kVectorKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (value.size() == Constant::kVectorKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(FATAL) << "Decode vector id failed, value size is not 9 or >=17, value:["
                     << codec::BytesToHexString(value) << "]";
    return 0;
  }

  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

static int64_t DecodePartitionId(const std::string& value) {
  Buf buf(value);

  // if (value.size() >= 17 || value.size() == 9) {
  if (value.size() >= Constant::kVectorKeyMaxLenWithPrefix || value.size() == Constant::kVectorKeyMinLenWithPrefix) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

}  // namespace vector_codec
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_CODEC_H_