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

#include <any>
#include <memory>
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
  std::shared_ptr<pb::meta::TableDefinition> td_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas_;
  RecordEncoder re_;
  RecordDecoder rd_;

 public:
  KeyValueCodec(std::shared_ptr<pb::meta::TableDefinition> td, uint64_t common_id);
  ~KeyValueCodec();

  int Decode(const std::string& key, const std::string& value, std::vector<std::any>& record /*output*/);
  int Decode(const std::string& key, const std::string& value, const std::vector<int>& column_indexes, std::vector<std::any>& record);

  int Decode(const pb::common::KeyValue& key_value, std::vector<std::any>& record /*output*/);
  int Decode(const pb::common::KeyValue& key_value, const std::vector<int>& column_indexes, std::vector<std::any>& record);
  
  int Decode(KeyValue& keyvalue, std::vector<std::any>& record /*output*/);

  int Encode(const std::vector<std::any>& record, std::string& key /*output*/, std::string& value /*output*/);
  int Encode(const std::vector<std::any>& record, pb::common::KeyValue& key_value /*output*/);
  int Encode(const std::vector<std::any>& record, KeyValue& keyvalue /*output*/);

  int EncodeKey(const std::vector<std::any>& record, std::string& output);

  int EncodeKeyPrefix(const std::vector<std::any>& record, int column_count, std::string& output);

  int EncodeMaxKeyPrefix(std::string& output);

  int EncodeMinKeyPrefix(std::string& output);
};

}  // namespace dingodb

#endif