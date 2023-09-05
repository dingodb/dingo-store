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

package io.dingodb.sdk.common.serial.schema;

import io.dingodb.sdk.common.serial.Buf;

import java.util.ArrayList;
import java.util.List;

public class IntegerListSchema implements DingoSchema<List<Integer>> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    public IntegerListSchema() {
    }

    public IntegerListSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.INTEGERLIST;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public boolean isKey() {
        return isKey;
    }

    @Override
    public int getLength() {
        if (allowNull) {
            return getWithNullTagLength();
        }
        return getDataLength();
    }

    private int getWithNullTagLength() {
        return 5;
    }

    private int getDataLength() {
        return 4;
    }

    @Override
    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    @Override
    public boolean isAllowNull() {
        return allowNull;
    }

    @Override
    public void encodeKey(Buf buf, List<Integer> data) {throw new RuntimeException("Array cannot be key");}

    @Override
    public void encodeKeyForUpdate(Buf buf, List<Integer> data) {throw new RuntimeException("Array cannot be key");}

    private void internalEncodeNull(Buf buf) {
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
    }

    private void internalEncodeKey(Buf buf, Integer data) {
        buf.write((byte) (data >>> 24 ^ 0x80));
        buf.write((byte) (data >>> 16));
        buf.write((byte) (data >>> 8));
        buf.write((byte) data.intValue());
    }

    @Override
    public List<Integer> decodeKey(Buf buf) {throw new RuntimeException("Array cannot be key");}

    @Override
    public List<Integer> decodeKeyPrefix(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void skipKey(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyPrefix(Buf buf, List<Integer> data) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeValue(Buf buf, List<Integer> data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                buf.ensureRemainder(5 + data.size() * 4);
                buf.write(NOTNULL);
                buf.writeInt(data.size());
                // 1
                for (Integer value: data) {
                    if(value == null) {
                        throw new IllegalArgumentException("Array type sub-elements do not support null values");
                    }
                    internalEncodeValue(buf, value);
                }
            }
        } else {
            buf.ensureRemainder(4 + data.size() * 4);
            buf.writeInt(data.size());
            for (Integer value: data) {
                if(value == null) {
                    throw new IllegalArgumentException("Array type sub-elements do not support null values");
                }
                internalEncodeValue(buf, value);
            }
        }
    }

    private void internalEncodeValue(Buf buf, Integer data) {
        buf.write((byte) (data >>> 24));
        buf.write((byte) (data >>> 16));
        buf.write((byte) (data >>> 8));
        buf.write((byte) data.intValue());
    }

    private Integer internalDecodeData (Buf buf){
        return (((buf.read() & 0xFF) << 24)
                | ((buf.read() & 0xFF) << 16)
                | ((buf.read() & 0xFF) << 8)
                | (buf.read() & 0xFF));
    }

    @Override
    public List<Integer> decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }

        int size = buf.readInt();
        List<Integer> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(internalDecodeData(buf));
        }
        return data;
    }

    @Override
    public void skipValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return;
            }
        }
        int length = buf.readInt();
        buf.skip(length * 4);
    }
}
