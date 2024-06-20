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
#include "common/serial_helper.h"
#include "fmt/core.h"

namespace dingodb {

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, std::string& result) {
  result.resize(Constant::kVectorKeyMinLenWithPrefix);
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
}

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result) {
  result.resize(Constant::kVectorKeyMaxLenWithPrefix);
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(vector_id, result);
}

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, int64_t ts,
                                  std::string& result) {
  result.resize(Constant::kVectorKeyMaxLenWithPrefix);
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(vector_id, result);
  SerialHelper::WriteLongWithNegation(ts, result);
}

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, const std::string& scalar_key,
                                  std::string& result) {
  if (BAIDU_UNLIKELY(scalar_key.empty())) {
    DINGO_LOG(FATAL) << fmt::format("scalar key is empty, {}/{}/{}", prefix, partition_id, vector_id);
  }

  result.resize(Constant::kVectorKeyMaxLenWithPrefix + scalar_key.size());
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(vector_id, result);
  result.append(scalar_key);
}

void VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, const std::string& scalar_key,
                                  int64_t ts, std::string& result) {
  if (BAIDU_UNLIKELY(scalar_key.empty())) {
    DINGO_LOG(FATAL) << fmt::format("scalar key is empty, {}/{}/{}", prefix, partition_id, vector_id);
  }

  result.resize(Constant::kVectorKeyMaxLenWithPrefix + scalar_key.size());
  result.push_back(prefix);
  SerialHelper::WriteLong(partition_id, result);
  SerialHelper::WriteLongComparable(vector_id, result);
  result.append(scalar_key);
  SerialHelper::WriteLongWithNegation(ts, result);
}

int64_t VectorCodec::DecodePartitionId(const std::string& key) {
  if (key.size() < Constant::kVectorKeyMinLenWithPrefix) {
    DINGO_LOG(FATAL) << fmt::format("Decode partition id failed, value({}) size too small", Helper::StringToHex(key));
  }

  return SerialHelper::ReadLong(key.substr(1, 9));
}

int64_t VectorCodec::DecodeVectorId(const std::string& key) {
  if (key.size() >= Constant::kVectorKeyMaxLenWithPrefix) {
    return SerialHelper::ReadLongComparable(
        key.substr(Constant::kVectorKeyMinLenWithPrefix, Constant::kVectorKeyMinLenWithPrefix + 8));

  } else if (key.size() == Constant::kVectorKeyMinLenWithPrefix) {
    return 0;

  } else {
    DINGO_LOG(FATAL) << fmt::format("Decode vector id failed, value({}) size too small", Helper::StringToHex(key));
    return 0;
  }
}

std::string VectorCodec::DecodeScalarKey(const std::string& key) {
  if (key.size() <= Constant::kVectorKeyMaxLenWithPrefix) {
    DINGO_LOG(FATAL) << fmt::format("Decode scalar key failed, value({}) size too small.", Helper::StringToHex(key));
    return "";
  }

  return key.substr(Constant::kVectorKeyMaxLenWithPrefix);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
std::string_view VectorCodec::TruncateTsForKey(const std::string& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  return std::string_view(key).substr(0, key.size() - 8);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
std::string_view VectorCodec::TruncateTsForKey(const std::string_view& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  return std::string_view(key).substr(0, key.size() - 8);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
int64_t VectorCodec::TruncateKeyForTs(const std::string& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  auto ts_str = key.substr(key.size() - 8, key.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

// key: prefix(1byes)|partition_id(8bytes)|vector_id(8bytes)|ts(8bytes)
int64_t VectorCodec::TruncateKeyForTs(const std::string_view& key) {
  CHECK(key.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(key));

  auto ts_str = key.substr(key.size() - 8, key.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
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
  return (key.size() == Constant::kVectorKeyMinLenWithPrefix || key.size() >= Constant::kVectorKeyMaxLenWithPrefix);
}

bool VectorCodec::IsLegalVectorId(int64_t vector_id) { return vector_id > 0 && vector_id != INT64_MAX; }

}  // namespace dingodb