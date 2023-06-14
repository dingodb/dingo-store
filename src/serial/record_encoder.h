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

#include <memory>
#include <vector>

#include "any"
#include "proto/common.pb.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

class RecordEncoder {
 private:
  int codec_version_ = 0;
  int schema_version_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas_;
  long common_id_;
  int key_buf_size_;
  int value_buf_size_;
  bool le_;

 public:
  RecordEncoder(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, long common_id);
  RecordEncoder(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, long common_id,
                bool le);

  void Init(int schema_version, std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas, long common_id);

  int Encode(const std::vector<std::any>& record, pb::common::KeyValue& key_value /*output*/);
  int Encode(const std::vector<std::any>& record, std::string& key, std::string& value);

  int EncodeKey(const std::vector<std::any>& record, std::string& output);

  int EncodeValue(const std::vector<std::any>& record, std::string& output);

  int EncodeKeyPrefix(const std::vector<std::any>& record, int column_count, std::string& output);

  int EncodeMaxKeyPrefix(std::string& output) const;

  int EncodeMinKeyPrefix(std::string& output) const;
};

}  // namespace dingodb

#endif