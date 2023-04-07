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

#ifndef DINGO_SERIAL_DOUBLE_SCHEMA_H_
#define DINGO_SERIAL_DOUBLE_SCHEMA_H_

#include "dingo_schema.h"
#include <iostream>
#include <cstring>

using namespace std;

namespace dingodb {

template <>

class DingoSchema<double*> : public BaseSchema
{
    private:
        int index;
        bool key, allow_null;

        int getDataLength();
        int getWithNullTagLength();
        void internalEncodeNull(Buf *buf);
        void internalEncodeKey(Buf *buf, double *data);
        void internalEncodeValue(Buf *buf, double *data);
    
    public:
        type getType();
        void setIndex(int index);
        int getIndex();
        void setIsKey(bool key);
        bool isKey();
        int getLength();
        void setAllowNull(bool allow_null);
        bool allowNull();
        void encodeKey(Buf *buf, double *data);
        double* decodeKey(Buf *buf);
        void skipKey(Buf *buf);
        void encodeValue(Buf *buf, double *data);
        double* decodeValue(Buf *buf);
        void skipValue(Buf *buf);
};

}

#endif