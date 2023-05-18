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

public class FloatSchema implements DingoSchema<Float> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    public FloatSchema() {
    }

    public FloatSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.FLOAT;
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
    public void encodeKey(Buf buf, Float data) {
        if (allowNull) {
            buf.ensureRemainder(getWithNullTagLength());
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeKey(buf, data);
            }
        } else {
            buf.ensureRemainder(getDataLength());
            internalEncodeKey(buf, data);
        }
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, Float data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeKey(buf, data);
            }
        } else {
            internalEncodeKey(buf, data);
        }
    }

    private void internalEncodeNull(Buf buf) {
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
    }

    private void internalEncodeKey(Buf buf, Float data) {
        int in = Float.floatToIntBits(data);
        if (data >= 0) {
            buf.write((byte) (in >>> 24 ^ 0x80));
            buf.write((byte) (in >>> 16));
            buf.write((byte) (in >>> 8));
            buf.write((byte) in);
        } else {
            buf.write((byte) (~ in >>> 24));
            buf.write((byte) (~ in >>> 16));
            buf.write((byte) (~ in >>> 8));
            buf.write((byte) ~ in);
        }
    }

    @Override
    public Float decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        int in = buf.read() & 0xFF;

        if (in >= 0x80) {
            in = in ^ 0x80;
            for (int i = 0; i < 3; i++) {
                in <<= 8;
                in |= buf.read() & 0xFF;
            }
        } else {
            in = ~in;
            for (int i = 0; i < 3; i++) {
                in <<= 8;
                in |= ~buf.read() & 0xFF;
            }
        }
        return Float.intBitsToFloat(in);
    }

    @Override
    public Float decodeKeyPrefix(Buf buf) {
        return decodeKey(buf);
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skip(getLength());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Float data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Float data) {
        if (allowNull) {
            buf.ensureRemainder(getWithNullTagLength());
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeValue(buf, data);
            }
        } else {
            buf.ensureRemainder(getDataLength());
            internalEncodeValue(buf, data);
        }
    }

    private void internalEncodeValue(Buf buf, Float data) {
        int in = Float.floatToIntBits(data);
        buf.write((byte) (in >>> 24));
        buf.write((byte) (in >>> 16));
        buf.write((byte) (in >>> 8));
        buf.write((byte) in);
    }

    @Override
    public Float decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        int in = buf.read() & 0xFF;
        for (int i = 0; i < 3; i++) {
            in <<= 8;
            in |= buf.read() & 0xFF;
        }
        return Float.intBitsToFloat(in);
    }

    @Override
    public void skipValue(Buf buf) {
        buf.skip(getLength());
    }
}
