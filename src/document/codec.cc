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

#include "document/codec.h"

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
void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    DINGO_LOG(FATAL) << "Encode document key failed, prefix is 0, partition_id:[" << partition_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kDocumentKeyMinLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  buf.GetBytes(result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    // Buf buf(16);
    // Buf buf(Constant::kDocumentKeyMaxLen);
    // buf.WriteLong(partition_id);
    // DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, document_id);
    // buf.GetBytes(result);

    // prefix == 0 is not allowed
    DINGO_LOG(FATAL) << "Encode document key failed, prefix is 0, partition_id:[" << partition_id << "], document_id:["
                     << document_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kDocumentKeyMaxLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, document_id);
  buf.GetBytes(result);
}

void DocumentCodec::EncodeDocumentKey(char prefix, int64_t partition_id, int64_t document_id,
                                      const std::string& scalar_key, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {  // prefix == 0 is not allowed
    DINGO_LOG(FATAL) << "Encode document key failed, prefix is 0, partition_id:[" << partition_id << "], document_id:["
                     << document_id << "]";
  }

  if (BAIDU_UNLIKELY(scalar_key.empty())) {
    DINGO_LOG(FATAL) << "Encode document key failed, scalar_key is empty, prefix:[" << prefix << "], partition_id:["
                     << partition_id << "], document_id:[" << document_id << "]";
  }

  // Buf buf(17 +  scalar_key.size());
  Buf buf(Constant::kDocumentKeyMaxLenWithPrefix + scalar_key.size());
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, document_id);
  buf.Write(scalar_key);
  buf.GetBytes(result);
}

int64_t DocumentCodec::DecodeDocumentId(const std::string& value) {
  Buf buf(value);
  if (value.size() >= Constant::kDocumentKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (value.size() == Constant::kDocumentKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(FATAL) << "Decode document id failed, value size is not 9 or >=17, value:[" << Helper::StringToHex(value)
                     << "]";
    return 0;
  }

  // return buf.ReadLong();
  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

int64_t DocumentCodec::DecodePartitionId(const std::string& value) {
  Buf buf(value);

  // if (value.size() >= 17 || value.size() == 9) {
  if (value.size() >= Constant::kDocumentKeyMaxLenWithPrefix ||
      value.size() == Constant::kDocumentKeyMinLenWithPrefix) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

std::string DocumentCodec::DecodeScalarKey(const std::string& value) {
  Buf buf(value);
  if (value.size() <= Constant::kDocumentKeyMaxLenWithPrefix) {
    DINGO_LOG(FATAL) << "Decode scalar key failed, value size <=17, value:[" << Helper::StringToHex(value) << "]";
    return "";
  }

  buf.Skip(Constant::kDocumentKeyMaxLenWithPrefix);

  return buf.ReadString();
}

std::string DocumentCodec::DecodeKeyToString(const std::string& key) {
  return fmt::format("{}_{}", DecodePartitionId(key), DecodeDocumentId(key));
}

std::string DocumentCodec::DecodeRangeToString(const pb::common::Range& range) {
  return fmt::format("[{}, {})", DecodeKeyToString(range.start_key()), DecodeKeyToString(range.end_key()));
}

void DocumentCodec::DecodeRangeToDocumentId(const pb::common::Range& range, int64_t& begin_document_id,
                                            int64_t& end_document_id) {
  begin_document_id = DocumentCodec::DecodeDocumentId(range.start_key());
  int64_t temp_end_document_id = DocumentCodec::DecodeDocumentId(range.end_key());
  if (temp_end_document_id > 0) {
    end_document_id = temp_end_document_id;
  } else {
    if (DecodePartitionId(range.end_key()) > DecodePartitionId(range.start_key())) {
      end_document_id = INT64_MAX;
    }
  }
}

bool DocumentCodec::IsValidKey(const std::string& key) {
  // return (key.size() == 8 || key.size() == 9 || key.size() == 16 || key.size() == 17);
  return (key.size() == Constant::kDocumentKeyMinLenWithPrefix || key.size() >= Constant::kDocumentKeyMaxLenWithPrefix);
}

bool DocumentCodec::IsLegalDocumentId(int64_t document_id) { return document_id > 0 && document_id != INT64_MAX; }

}  // namespace dingodb