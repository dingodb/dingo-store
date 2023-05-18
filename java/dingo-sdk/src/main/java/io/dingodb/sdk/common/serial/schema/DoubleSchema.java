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

public class DoubleSchema implements DingoSchema<Double> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    public DoubleSchema() {
    }

    public DoubleSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.DOUBLE;
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
    public void encodeKey(Buf buf, Double data) {
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
    public void encodeKeyForUpdate(Buf buf, Double data) {
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
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
    }

    private void internalEncodeKey(Buf buf, Double data) {
        long ln = (Double.doubleToLongBits(data));
        if (data >= 0) {
            buf.write((byte) (ln >>> 56 ^ 0x80));
            buf.write((byte) (ln >>> 48));
            buf.write((byte) (ln >>> 40));
            buf.write((byte) (ln >>> 32));
            buf.write((byte) (ln >>> 24));
            buf.write((byte) (ln >>> 16));
            buf.write((byte) (ln >>> 8));
            buf.write((byte) ln);
        } else {
            buf.write((byte) (~ ln >>> 56));
            buf.write((byte) (~ ln >>> 48));
            buf.write((byte) (~ ln >>> 40));
            buf.write((byte) (~ ln >>> 32));
            buf.write((byte) (~ ln >>> 24));
            buf.write((byte) (~ ln >>> 16));
            buf.write((byte) (~ ln >>> 8));
            buf.write((byte) ~ ln);
        }
    }

    @Override
    public Double decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        long l = buf.read() & 0xFF;

        if (l >= 0x80) {
            l = l ^ 0x80;
            for (int i = 0; i < 7; i++) {
                l <<= 8;
                l |= buf.read() & 0xFF;
            }
        } else {
            l = ~l;
            for (int i = 0; i < 7; i++) {
                l <<= 8;
                l |= ~buf.read() & 0xFF;
            }
        }
        return Double.longBitsToDouble(l);
    }

    @Override
    public Double decodeKeyPrefix(Buf buf) {
        return decodeKey(buf);
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skip(getLength());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Double data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Double data) {
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

    @Override
    public Double decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        long l = buf.read()  & 0xFF;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf.read() & 0xFF;
        }
        return Double.longBitsToDouble(l);
    }

    @Override
    public void skipValue(Buf buf) {
        buf.skip(getLength());
    }
}
