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

void VectorCodec::EncodeVectorWal(uint64_t region_id, uint64_t vector_id, uint64_t log_id, std::string& result) {
  Buf buf(25);
  buf.WriteLong(region_id);
  buf.Write(Constant::kVectorWalPrefix);
  buf.WriteLong(log_id);
  buf.WriteLong(vector_id);

  buf.GetBytes(result);
}

int VectorCodec::DecodeVectorWal(const std::string& value, uint64_t& vector_id, uint64_t& log_id) {
  if (value.size() != 25) {
    DINGO_LOG(ERROR) << "DecodeVectorIdFromWal failed, value size is not 8, value:[" << value << "]";
    return -1;
  }
  Buf buf(value);
  buf.ReadLong();              // region_id
  buf.Read();                  // kVectorWalPrefix
  log_id = buf.ReadLong();     // log_id
  vector_id = buf.ReadLong();  // vector_id

  return 0;
}

std::string VectorCodec::EncodeVectorIndexLogIndex(uint64_t snapshot_log_index, uint64_t apply_log_index) {
  Buf buf(16);
  buf.WriteLong(snapshot_log_index);
  buf.WriteLong(apply_log_index);

  std::string result;
  buf.GetBytes(result);
  return result;
}

int VectorCodec::DecodeVectorIndexLogIndex(const std::string& value, uint64_t& snapshot_log_index,
                                           uint64_t& apply_log_index) {
  if (value.size() != 16) {
    DINGO_LOG(ERROR) << "DecodeVectorApplyLogIndex failed, value size is not 16, value:[" << value
                     << "], size=" << value.size();
    return -1;
  }
  Buf buf(value);
  snapshot_log_index = buf.ReadLong();
  apply_log_index = buf.ReadLong();

  return 0;
}

}  // namespace dingodb