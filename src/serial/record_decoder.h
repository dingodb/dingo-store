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

#ifndef DINGO_SERIAL_RECORD_DECODER_H_
#define DINGO_SERIAL_RECORD_DECODER_H_

#include <memory>

#include "any"
#include "functional"
#include "keyvalue.h"
#include "optional"
#include "proto/common.pb.h"
#include "schema/boolean_schema.h"
#include "schema/double_schema.h"
#include "schema/float_schema.h"
#include "schema/integer_schema.h"
#include "schema/long_schema.h"
#include "schema/string_schema.h"
#include "schema/boolean_list_schema.h"
#include "schema/string_list_schema.h"
#include "schema/double_list_schema.h"
#include "schema/float_list_schema.h"
#include "schema/integer_list_schema.h"
#include "schema/long_list_schema.h"
#include "utils.h"

namespace dingodb {

class RecordDecoder {
 private:
  bool CheckPrefix(Buf* buf) const;
  bool CheckReverseTag(Buf* buf) const;
  bool CheckSchemaVersion(Buf* buf) const;

  int codec_version_ = 1;
  int schema_version_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas_;
  long common_id_;
  bool le_;

 public:
  RecordDecoder(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, long common_id);
  RecordDecoder(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, long common_id,
                bool le);

  void Init(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, long common_id);

  int Decode(const KeyValue& key_value, std::vector<std::any>& record /*output*/);
  int Decode(const pb::common::KeyValue& key_value, std::vector<std::any>& record /*output*/);
  int Decode(const std::string& key, const std::string& value, std::vector<std::any>& record /*output*/);
  int DecodeKey(const std::string& key, std::vector<std::any>& record /*output*/);

  int Decode(const KeyValue& key_value, const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/);
  int Decode(const pb::common::KeyValue& key_value, const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/);
  int Decode(const std::string& key, const std::string& value, const std::vector<int>& column_indexes,
             std::vector<std::any>& record /*output*/);
};

}  // namespace dingodb

#endif
