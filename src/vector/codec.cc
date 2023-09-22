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

namespace dingodb {

void VectorCodec::EncodeVectorKey(uint64_t partition_id, uint64_t vector_id, std::string& result) {
  Buf buf(16);
  buf.WriteLong(partition_id);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

void VectorCodec::EncodeVectorData(uint64_t partition_id, uint64_t vector_id, std::string& result) {
  Buf buf(17);
  buf.Write(Constant::kVectorDataPrefix);
  buf.WriteLong(partition_id);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

void VectorCodec::EncodeVectorScalar(uint64_t partition_id, uint64_t vector_id, std::string& result) {
  Buf buf(17);
  buf.Write(Constant::kVectorScalarPrefix);
  buf.WriteLong(partition_id);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

void VectorCodec::EncodeVectorTable(uint64_t partition_id, uint64_t vector_id, std::string& result) {
  Buf buf(17);
  buf.Write(Constant::kVectorTablePrefix);
  buf.WriteLong(partition_id);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

uint64_t VectorCodec::DecodeVectorId(const std::string& value) {
  Buf buf(value);
  if (value.size() == 17) {
    buf.Skip(9);
  } else if (value.size() == 16) {
    buf.Skip(8);
  } else if (value.size() == 9 || value.size() == 8) {
    return 0;
  } else {
    DINGO_LOG(ERROR) << "Decode vector id failed, value size is not 16 or 17, value:[" << Helper::StringToHex(value)
                     << "]";
    return 0;
  }

  return buf.ReadLong();
}

uint64_t VectorCodec::DecodePartitionId(const std::string& value) {
  Buf buf(value);

  if (value.size() == 17) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

std::string VectorCodec::FillVectorDataPrefix(const std::string& value) {
  Buf buf(17);
  buf.Write(Constant::kVectorDataPrefix);
  buf.Write(value);

  return buf.GetString();
}

std::string VectorCodec::FillVectorScalarPrefix(const std::string& value) {
  Buf buf(17);
  buf.Write(Constant::kVectorScalarPrefix);
  buf.Write(value);

  return buf.GetString();
}

std::string VectorCodec::FillVectorTablePrefix(const std::string& value) {
  Buf buf(17);
  buf.Write(Constant::kVectorTablePrefix);
  buf.Write(value);

  return buf.GetString();
}

std::string VectorCodec::RemoveVectorPrefix(const std::string& value) { return value.substr(1); }  // NOLINT

bool VectorCodec::IsValidKey(const std::string& key) {
  return (key.size() == 8 || key.size() == 9 || key.size() == 16 || key.size() == 17);
}

}  // namespace dingodb