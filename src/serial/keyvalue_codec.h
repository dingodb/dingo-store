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

#ifndef DINGO_SERIAL_KEYVALUE_CODEC_H_
#define DINGO_SERIAL_KEYVALUE_CODEC_H_

#include <string>
#include <vector>

#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "serial/keyvalue.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

class KeyValueCodec {
 private:
  pb::meta::TableDefinition* td_;
  std::vector<BaseSchema*>* schemas_;
  RecordEncoder* re_;
  RecordDecoder* rd_;

 public:
  KeyValueCodec(pb::meta::TableDefinition* td, uint64_t common_id);
  ~KeyValueCodec();

  std::vector<std::any>* Decode(KeyValue* keyvalue);
  KeyValue* Encode(std::vector<std::any>* record);

  int Decode(const std::string& key, const std::string& value, std::vector<std::any>& record /*output*/);
  int Encode(const std::vector<std::any>& record, std::string& key /*output*/, std::string& value /*output*/);

  int Decode(const pb::common::KeyValue& key_value, std::vector<std::any>& record /*output*/);
  int Encode(const std::vector<std::any>& record, pb::common::KeyValue& key_value /*output*/);

  std::string* EncodeKey(std::vector<std::any>* record);
  int EncodeKey(const std::vector<std::any>& record, std::string& output);
  std::string* EncodeKeyPrefix(std::vector<std::any>* record, int column_count);
  int EncodeKeyPrefix(const std::vector<std::any>& record, int column_count, std::string& output);
  std::string* EncodeMaxKeyPrefix();
  int EncodeMaxKeyPrefix(std::string& output);
  std::string* EncodeMinKeyPrefix();
  int EncodeMinKeyPrefix(std::string& output);
};

}  // namespace dingodb

#endif