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

#ifndef DINGO_SERIAL_BUF_H_
#define DINGO_SERIAL_BUF_H_

#include <iostream>
#include <vector>
#include <string>

using namespace std;

namespace dingodb {

class Buf {
    private:
        vector<uint8_t> buf;
        int forward_pos = 0;
        int reverse_pos = 0;
    public:
        Buf(int size);
        Buf(string buf);
        void setForwardPos(int fp);
        void setReversePos(int rp);
        vector<uint8_t>* getBuf();
        void write(uint8_t b);
        void writeInt(int i);
        void writeLong(long l);
        void reverseWrite(uint8_t b);
        void reverseWriteInt(int i);
        uint8_t read();
        int readInt();
        long readLong();
        uint8_t reverseRead();
        int reverseReadInt();
        void reverseSkipInt();
        void skip(int size);
        void reverseSkip(int size);
        void ensureRemainder(int length);
        string* getBytes();
};

}

#endif