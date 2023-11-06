/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.common.serial;

public interface Buf {

    void write(byte b);
    void write(byte[] b);
    void write(byte[] b, int pos, int length);
    void writeInt(int i);
    void writeLong(long l);

    byte peek();
    int peekInt();
    long peekLong();

    byte read();
    byte[] read(int length);
    void read(byte[] b, int pos, int length);
    int readInt();
    long readLong();

    void reverseWrite(byte b);
    byte reverseRead();
    void reverseWriteInt(int i);
    void reverseWriteInt0();
    int reverseReadInt();
    void skip(int length);
    void reverseSkip(int length);
    void reverseSkipInt();
    void ensureRemainder(int length);
    void resize(int oldSize, int newSize);

    boolean isEnd();

    byte[] getBytes();
}
