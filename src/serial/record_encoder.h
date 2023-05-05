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

#ifndef DINGO_SERIAL_RECORD_ENCODER_H_
#define DINGO_SERIAL_RECORD_ENCODER_H_

#include "any"
#include "optional"
#include "functional"
#include "keyvalue.h"
#include "schema/boolean_schema.h"
#include "schema/double_schema.h"
#include "schema/integer_schema.h"
#include "schema/long_schema.h"
#include "schema/string_schema.h"
#include "utils.h"

namespace dingodb {

class RecordEncoder {
 private:
  int codec_version_ = 0;
  int schema_version_;
  std::vector<BaseSchema*>* schemas_;
  long common_id_;
  int key_buf_size_;
  int value_buf_size_;
  bool le_;

 public:
  RecordEncoder(int schema_version, std::vector<BaseSchema*>* schemas,
                long common_id);
  RecordEncoder(int schema_version, std::vector<BaseSchema*>* schemas,
                long common_id, bool le);
  void Init(int schema_version, std::vector<BaseSchema*>* schemas,
                long common_id);
  KeyValue* Encode(std::vector<std::any>* record);
  std::string* EncodeKey(std::vector<std::any>* record);
  std::string* EncodeValue(std::vector<std::any>* record);
  std::string* EncodeKeyPrefix(std::vector<std::any>* record, int column_count);
  std::string* EncodeMaxKeyPrefix() const;
  std::string* EncodeMinKeyPrefix() const;
};

}  // namespace dingodb

#endif