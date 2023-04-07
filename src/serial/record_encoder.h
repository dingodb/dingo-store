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

#include "schema/boolean_schema.h"
#include "schema/double_schema.h"
#include "schema/integer_schema.h"
#include "schema/long_schema.h"
#include "schema/string_schema.h"
#include "keyvalue.h"
#include "utils.h"
#include "any"

namespace dingodb {

class RecordEncoder {
    private:
        int codec_version = 0;
        int schema_version;
        vector<BaseSchema*> *schemas;
        long common_id;
        int key_buf_size;
        int value_buf_size;

    public:
        RecordEncoder(int schema_version, vector<BaseSchema*> *schemas, long common_id);
        KeyValue* encode(vector<any> record);
        string encodeKey(vector<any> record);
        string encodeValue(vector<any> record);
};

}

#endif