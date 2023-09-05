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

public class DoubleListSchema implements DingoSchema<List<Double>> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    public DoubleListSchema() {
    }

    public DoubleListSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.DOUBLELIST;
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
        return 9;
    }

    private int getDataLength() {
        return 8;
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
    public void encodeKey(Buf buf, List<Double> data) {throw new RuntimeException("Array cannot be key");}

    @Override
    public void encodeKeyForUpdate(Buf buf, List<Double> data) {throw new RuntimeException("Array cannot be key");}

    @Override
    public List<Double> decodeKey(Buf buf) {throw new RuntimeException("Array cannot be key");}

    @Override
    public List<Double> decodeKeyPrefix(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void skipKey(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyPrefix(Buf buf, List<Double> data) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeValue(Buf buf, List<Double> data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                buf.ensureRemainder(9 + data.size() * 8);
                buf.write(NOTNULL);
                buf.writeInt(data.size());
                for (Double value: data) {
                    if(value == null) {
                        throw new IllegalArgumentException("Array type sub-elements do not support null values");
                    }
                    internalEncodeValue(buf, value);
                }
            }
        } else {
            buf.ensureRemainder(8 + data.size() * 8);
            buf.writeInt(data.size());
            for (Double value: data) {
                if(value == null) {
                    throw new IllegalArgumentException("Array type sub-elements do not support null values");
                }
                internalEncodeValue(buf, value);
            }
        }
    }

    private void internalEncodeValue(Buf buf, Double data) {
        long ln = Double.doubleToLongBits(data);
        buf.write((byte) (ln >>> 56));
        buf.write((byte) (ln >>> 48));
        buf.write((byte) (ln >>> 40));
        buf.write((byte) (ln >>> 32));
        buf.write((byte) (ln >>> 24));
        buf.write((byte) (ln >>> 16));
        buf.write((byte) (ln >>> 8));
        buf.write((byte) ln);
    }

    private Double internalDecodeData (Buf buf){
        long l = buf.read()  & 0xFF;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf.read() & 0xFF;
        }
        return Double.longBitsToDouble(l);
    }

    @Override
    public List<Double> decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }

        int size = buf.readInt();
        List<Double> data = new ArrayList<>(size);
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
        buf.skip(length * 8);
    }
}
