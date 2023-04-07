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

#ifndef DINGO_SERIAL_BASE_SCHEMA_H_
#define DINGO_SERIAL_BASE_SCHEMA_H_

#include <cstdint>

namespace dingodb {

class BaseSchema {
    protected:
        const uint8_t kNull = 0;
        const uint8_t kNotNull = 1;
    public:
        enum type {Bool, Integer, Long, Double, String};
        virtual type getType() = 0;
        virtual bool allowNull() = 0;
        virtual int getLength() = 0;
        virtual bool isKey() = 0;
};

}

#endif