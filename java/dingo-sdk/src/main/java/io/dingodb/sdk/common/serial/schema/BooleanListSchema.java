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
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.Type;

import java.util.ArrayList;
import java.util.List;

public class BooleanListSchema implements DingoSchema<List<Boolean>> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    public BooleanListSchema() {
    }

    public BooleanListSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.BOOLEANLIST;
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
    public int getWithNullTagLength() {
        return 2;
    }

    @Override
    public int getLength() {
        if (allowNull) {
            return getWithNullTagLength();
        }
        return getDataLength();
    }

    @Override
    public int getValueLengthV2() {
        return 0;
    }

    private int getDataLength() {
        return 1;
    }

    @Override
    public void setAllowNull(boolean allowNull) {
        this.allowNull = allowNull;
    }

    @Override
    public boolean isAllowNull() {
        return allowNull;
    }

    public void encodeKey(Buf buf, List<Boolean> data) {
        throw new RuntimeException("Array cannot be key");
    }

    public void encodeKeyV2(Buf buf, List<Boolean> data) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, List<Boolean> data) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyForUpdateV2(Buf buf, List<Boolean> data) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public List<Boolean> decodeKey(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public List<Boolean> decodeKeyV2(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public List<Boolean> decodeKeyPrefix(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void skipKey(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void skipKeyV2(Buf buf) {
        throw new RuntimeException("Array cannot be key");
    }

    @Override
    public void encodeKeyPrefix(Buf buf, List<Boolean> data) {
        throw new RuntimeException("Array cannot be key");
    }

    private void internalEncodeData(Buf buf, Boolean boolVal) {
        if (boolVal) {
            buf.write((byte) 1);
        } else {
            buf.write((byte) 0);
        }
    }

    @Override
    public void encodeValue(Buf buf, List<Boolean> data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                buf.ensureRemainder(5 + data.size());
                buf.write(NOTNULL);
                buf.writeInt(data.size());
                for (Boolean value: data) {
                    if (value == null) {
                        throw new IllegalArgumentException("Array type sub-elements do not support null values");
                    }
                    internalEncodeData(buf, value);
                }
            }
        } else {
            buf.ensureRemainder(4 + data.size());
            buf.writeInt(data.size());
            for (Boolean value: data) {
                if (value == null) {
                    throw new IllegalArgumentException("Array type sub-elements do not support null values");
                }
                internalEncodeData(buf, value);
            }
        }
    }

    @Override
    public int encodeValueV2(Buf buf, List<Boolean> data) {
        int len = 0;

        if (allowNull) {
            if (data == null) {
                return 0;
            } else {
                len = 4 + data.size();
                buf.ensureRemainder(len);

                buf.writeInt(data.size());
                for (Boolean value: data) {
                    if (value == null) {
                        throw new IllegalArgumentException("Array type sub-elements do not support null values");
                    }
                    internalEncodeData(buf, value);
                }
            }
        } else {
            len = 4 + data.size();
            buf.ensureRemainder(len);

            buf.writeInt(data.size());
            for (Boolean value: data) {
                if (value == null) {
                    throw new IllegalArgumentException("Array type sub-elements do not support null values");
                }
                internalEncodeData(buf, value);
            }
        }

        return len;
    }

    @Override
    public List<Boolean> decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }
        int size = buf.readInt();
        List<Boolean> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(internalDecodeData(buf));
        }
        return data;
    }

    @Override
    public List<Boolean> decodeValueV2(Buf buf) {
        int size = buf.readInt();
        List<Boolean> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            data.add(internalDecodeData(buf));
        }
        return data;
    }

    private Boolean internalDecodeData(Buf buf) {
        return buf.read() == (byte) 0 ? false : true;
    }

    @Override
    public void skipValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return;
            }
        }
        int length = buf.readInt();
        buf.skip(length);
    }

    @Override
    public void skipValueV2(Buf buf) {
        int length = buf.readInt();
        buf.skip(length);
    }
}

