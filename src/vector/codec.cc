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
#include "common/logging.h"
#include "serial/buf.h"

namespace dingodb {

void VectorCodec::EncodeVectorId(uint64_t region_id, uint64_t vector_id, std::string& result) {
  Buf buf(17);
  buf.WriteLong(region_id);
  buf.Write(Constant::kVectorIdPrefix);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

uint64_t VectorCodec::DecodeVectorId(const std::string& value) {
  if (value.size() != 17) {
    DINGO_LOG(ERROR) << "DecodeVectorId failed, value size is not 8, value:[" << value << "]";
    return 0;
  }
  Buf buf(value);
  buf.ReadLong();
  buf.Read();

  return buf.ReadLong();
}

void VectorCodec::EncodeVectorMeta(uint64_t region_id, uint64_t vector_id, std::string& result) {
  Buf buf(17);
  buf.WriteLong(region_id);
  buf.Write(Constant::kVectorMetaPrefix);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

std::string VectorCodec::EncodeVecotrIndexLogIndex(uint64_t snapshot_log_index, uint64_t apply_log_index) {
  Buf buf(16);
  buf.WriteLong(snapshot_log_index);
  buf.WriteLong(apply_log_index);

  std::string result;
  buf.GetBytes(result);
  return result;
}

uint64_t VectorCodec::DecodeVectorSnapshotLogIndex(const std::string& value) {
  if (value.size() != 18) {
    DINGO_LOG(ERROR) << "DecodeVectorSnapshotLogIndex failed, value size is not 8, value:[" << value << "]";
    return 0;
  }
  Buf buf(value);

  return buf.ReadLong();
}

uint64_t VectorCodec::DecodeVectorApplyLogIndex(const std::string& value) {
  if (value.size() != 18) {
    DINGO_LOG(ERROR) << "DecodeVectorApplyLogIndex failed, value size is not 8, value:[" << value << "]";
    return 0;
  }
  Buf buf(value);
  buf.ReadLong();

  return buf.ReadLong();
}

}  // namespace dingodb