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
#include <string>
#include <utility>

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/serial_helper.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mvcc/codec.h"

namespace dingodb {

std::string VectorCodec::PackageVectorKey(char prefix, int64_t partition_id) {
  CHECK(prefix != 0) << fmt::format("Invalid prefix {}.", prefix);
  CHECK(partition_id > 0) << fmt::format("Invalid partition_id {}.", partition_id);

  std::string plain_key;
  plain_key.reserve(Constant::kVectorKeyMinLenWithPrefix);

  plain_key.push_back(prefix);
  SerialHelper::WriteLong(partition_id, plain_key);

  return std::move(plain_key);
}

std::string VectorCodec::PackageVectorKey(char prefix, int64_t partition_id, int64_t vector_id) {
  CHECK(prefix != 0) << fmt::format("Invalid prefix {}.", prefix);
  CHECK(partition_id > 0) << fmt::format("Invalid partition_id {}.", partition_id);
  CHECK(vector_id >= 0) << fmt::format("Invalid vector_id {}.", vector_id);

  std::string plain_key;
  plain_key.reserve(Constant::kVectorKeyMaxLenWithPrefix);

  plain_key.push_back(prefix);
  SerialHelper::WriteLong(partition_id, plain_key);
  SerialHelper::WriteLongComparable(vector_id, plain_key);

  return std::move(plain_key);
}

std::string VectorCodec::PackageVectorKey(char prefix, int64_t partition_id, int64_t vector_id,
                                          const std::string& scalar_key) {
  CHECK(prefix != 0) << fmt::format("Invalid prefix {}.", prefix);
  CHECK(partition_id > 0) << fmt::format("Invalid partition_id {}.", partition_id);
  CHECK(vector_id >= 0) << fmt::format("Invalid vector_id {}.", vector_id);
  CHECK(!scalar_key.empty()) << fmt::format("Scalar key is empty, {}/{}/{}.", prefix, partition_id, vector_id);

  std::string plain_key;
  plain_key.reserve(Constant::kVectorKeyMaxLenWithPrefix + scalar_key.size());

  plain_key.push_back(prefix);
  SerialHelper::WriteLong(partition_id, plain_key);
  SerialHelper::WriteLongComparable(vector_id, plain_key);
  plain_key.append(scalar_key);

  return std::move(plain_key);
}

int64_t VectorCodec::UnPackagePartitionId(const std::string& plain_key) {
  CHECK(plain_key.size() >= Constant::kVectorKeyMinLenWithPrefix)
      << fmt::format("Decode partition id failed, value({}) size too small", Helper::StringToHex(plain_key));

  return SerialHelper::ReadLong(plain_key.substr(1, 9));
}

int64_t VectorCodec::UnPackageVectorId(const std::string& plain_key) {
  if (plain_key.size() >= Constant::kVectorKeyMaxLenWithPrefix) {
    return SerialHelper::ReadLongComparable(plain_key.substr(9, 17));

  } else if (plain_key.size() == Constant::kVectorKeyMinLenWithPrefix) {
    return 0;

  } else {
    DINGO_LOG(FATAL) << fmt::format("Decode vector id failed, value({}) size too small",
                                    Helper::StringToHex(plain_key));
    return 0;
  }
}

std::string VectorCodec::UnPackageScalarKey(const std::string& plain_key) {
  CHECK(plain_key.size() > Constant::kVectorKeyMaxLenWithPrefix)
      << fmt::format("Decode scalar key failed, value({}) size too small.", Helper::StringToHex(plain_key));

  return plain_key.substr(Constant::kVectorKeyMaxLenWithPrefix);
}

std::string VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id) {
  std::string plain_key = PackageVectorKey(prefix, partition_id);

  return mvcc::Codec::EncodeBytes(plain_key);
}

std::string VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id) {
  std::string plain_key = PackageVectorKey(prefix, partition_id, vector_id);

  return mvcc::Codec::EncodeBytes(plain_key);
}

std::string VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id, int64_t ts) {
  std::string plain_key = PackageVectorKey(prefix, partition_id, vector_id);

  return mvcc::Codec::EncodeKey(plain_key, ts);
}

std::string VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id,
                                         const std::string& scalar_key) {
  std::string plain_key = PackageVectorKey(prefix, partition_id, vector_id, scalar_key);

  return mvcc::Codec::EncodeBytes(plain_key);
}

std::string VectorCodec::EncodeVectorKey(char prefix, int64_t partition_id, int64_t vector_id,
                                         const std::string& scalar_key, int64_t ts) {
  std::string plain_key = PackageVectorKey(prefix, partition_id, vector_id, scalar_key);

  return mvcc::Codec::EncodeKey(plain_key, ts);
}

int64_t VectorCodec::DecodePartitionIdFromEncodeKey(const std::string& encode_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key));

  return UnPackagePartitionId(plain_key);
}

int64_t VectorCodec::DecodePartitionIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  return UnPackagePartitionId(plain_key);
}

int64_t VectorCodec::DecodeVectorIdFromEncodeKey(const std::string& encode_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key));

  return UnPackageVectorId(plain_key);
}

int64_t VectorCodec::DecodeVectorIdFromEncodeKeyWithTs(const std::string& encode_key_with_ts) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  return UnPackageVectorId(plain_key);
}

std::string VectorCodec::DecodeScalarKeyFromEncodeKey(const std::string& encode_key) {
  CHECK(encode_key.size() >= 27) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key));

  return UnPackageScalarKey(plain_key);
}
std::string VectorCodec::DecodeScalarKeyFromEncodeKeyWithTs(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 35) << "encode_key_with_ts length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  return UnPackageScalarKey(plain_key);
}

void VectorCodec::DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& vector_id) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key));

  partition_id = UnPackagePartitionId(plain_key);
  vector_id = UnPackageVectorId(plain_key);
}

void VectorCodec::DecodeFromEncodeKey(const std::string& encode_key, int64_t& partition_id, int64_t& vector_id,
                                      std::string& scalar_key) {
  CHECK((encode_key.size() == 18 || encode_key.size() >= 27)) << "encode_key length is invalid.";

  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(encode_key, plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key));

  partition_id = UnPackagePartitionId(plain_key);
  vector_id = UnPackageVectorId(plain_key);
  scalar_key = UnPackageScalarKey(plain_key);
}

void VectorCodec::DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                            int64_t& vector_id) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";
  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  partition_id = UnPackagePartitionId(plain_key);
  vector_id = UnPackageVectorId(plain_key);
}

void VectorCodec::DecodeFromEncodeKeyWithTs(const std::string& encode_key_with_ts, int64_t& partition_id,
                                            int64_t& vector_id, std::string& scalar_key) {
  CHECK((encode_key_with_ts.size() == 26 || encode_key_with_ts.size() >= 35))
      << "encode_key_with_ts length is invalid.";
  std::string plain_key;
  bool ret = mvcc::Codec::DecodeBytes(TruncateTsForKey(encode_key_with_ts), plain_key);
  CHECK(ret) << fmt::format("Decode vector key({}) fail.", Helper::StringToHex(encode_key_with_ts));

  partition_id = UnPackagePartitionId(plain_key);
  vector_id = UnPackageVectorId(plain_key);
  scalar_key = UnPackageScalarKey(plain_key);
}

std::string_view VectorCodec::TruncateTsForKey(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  return std::string_view(encode_key_with_ts).substr(0, encode_key_with_ts.size() - 8);
}

std::string_view VectorCodec::TruncateTsForKey(const std::string_view& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 25) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  return std::string_view(encode_key_with_ts).substr(0, encode_key_with_ts.size() - 8);
}

int64_t VectorCodec::TruncateKeyForTs(const std::string& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 8) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  auto ts_str = encode_key_with_ts.substr(encode_key_with_ts.size() - 8, encode_key_with_ts.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

int64_t VectorCodec::TruncateKeyForTs(const std::string_view& encode_key_with_ts) {
  CHECK(encode_key_with_ts.size() >= 8) << fmt::format("Key({}) is invalid.", Helper::StringToHex(encode_key_with_ts));

  auto ts_str = encode_key_with_ts.substr(encode_key_with_ts.size() - 8, encode_key_with_ts.size());

  return SerialHelper::ReadLongWithNegation(ts_str);
}

std::string VectorCodec::DebugKey(bool is_encode, const std::string& key) {
  if (is_encode) {
    return fmt::format("{}_{}", DecodePartitionIdFromEncodeKey(key), DecodeVectorIdFromEncodeKey(key));
  } else {
    return fmt::format("{}_{}", UnPackagePartitionId(key), UnPackageVectorId(key));
  }
}

std::string VectorCodec::DebugRange(bool is_encode, const pb::common::Range& range) {
  return fmt::format("[{}, {})", DebugKey(is_encode, range.start_key()), DebugKey(is_encode, range.end_key()));
}

void VectorCodec::DebugRange(bool is_encode, const pb::common::Range& range, std::string& start_key,
                             std::string& end_key) {
  start_key = DebugKey(is_encode, range.start_key());
  end_key = DebugKey(is_encode, range.start_key());
}

void VectorCodec::DecodeRangeToVectorId(bool is_encode, const pb::common::Range& range, int64_t& begin_vector_id,
                                        int64_t& end_vector_id) {
  if (is_encode) {
    begin_vector_id = VectorCodec::DecodeVectorIdFromEncodeKey(range.start_key());
    end_vector_id = VectorCodec::DecodeVectorIdFromEncodeKey(range.end_key());
    if (end_vector_id == 0 &&
        (DecodePartitionIdFromEncodeKey(range.end_key()) > DecodePartitionIdFromEncodeKey(range.start_key()))) {
      end_vector_id = INT64_MAX;
    }

  } else {
    begin_vector_id = VectorCodec::UnPackageVectorId(range.start_key());
    end_vector_id = VectorCodec::UnPackageVectorId(range.end_key());
    if (end_vector_id == 0 && (UnPackagePartitionId(range.end_key()) > UnPackagePartitionId(range.start_key()))) {
      end_vector_id = INT64_MAX;
    }
  }
}

bool VectorCodec::IsValidKey(const std::string& key) {
  return (key.size() == Constant::kVectorKeyMinLenWithPrefix || key.size() >= Constant::kVectorKeyMaxLenWithPrefix);
}

bool VectorCodec::IsLegalVectorId(int64_t vector_id) { return vector_id > 0 && vector_id != INT64_MAX; }

}  // namespace dingodb