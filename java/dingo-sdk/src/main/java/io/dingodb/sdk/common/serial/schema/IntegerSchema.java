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
import lombok.Getter;
import lombok.Setter;

public class IntegerSchema implements DingoSchema<Integer> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    @Getter
    @Setter
    private long precision;

    @Getter
    @Setter
    private long scale;

    public IntegerSchema() {
    }

    public IntegerSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.INTEGER;
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

    @Override
    public int getValueLengthV2() {
        return getDataLength();
    }

    @Override
    public int getWithNullTagLength() {
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
    public void encodeKey(Buf buf, Integer data) {
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
    public void encodeKeyV2(Buf buf, Integer data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, Integer data) {
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

    @Override
    public void encodeKeyForUpdateV2(Buf buf, Integer data) {
        encodeKeyForUpdate(buf, data);
    }

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
    public Integer decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return (((buf.read() & 0xFF ^ 0x80) << 24)
                | ((buf.read() & 0xFF) << 16)
                | ((buf.read() & 0xFF) << 8)
                | (buf.read() & 0xFF));
    }

    @Override
    public Integer decodeKeyV2(Buf buf) {
        return decodeKey(buf);
    }

    @Override
    public Integer decodeKeyPrefix(Buf buf) {
        return decodeKey(buf);
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skip(getLength());
    }

    @Override
    public void skipKeyV2(Buf buf) {
        skipKey(buf);
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Integer data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Integer data) {
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

    @Override
    public int encodeValueV2(Buf buf, Integer data) {
        int len = getValueLengthV2();
        buf.ensureRemainder(len);
        if (allowNull) {
            if (data == null) {
                return 0;
            } else {
                internalEncodeValue(buf, data);
            }
        } else {
            internalEncodeValue(buf, data);
        }

        return len;
    }

    private void internalEncodeValue(Buf buf, Integer data) {
        buf.write((byte) (data >>> 24));
        buf.write((byte) (data >>> 16));
        buf.write((byte) (data >>> 8));
        buf.write((byte) data.intValue());
    }

    @Override
    public Integer decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return (((buf.read() & 0xFF) << 24)
                | ((buf.read() & 0xFF) << 16)
                | ((buf.read() & 0xFF) << 8)
                | (buf.read() & 0xFF));
    }

    @Override
    public Integer decodeValueV2(Buf buf) {
        return (((buf.read() & 0xFF) << 24)
                | ((buf.read() & 0xFF) << 16)
                | ((buf.read() & 0xFF) << 8)
                | (buf.read() & 0xFF));
    }

    @Override
    public void skipValue(Buf buf) {
        buf.skip(getLength());
    }

    @Override
    public void skipValueV2(Buf buf) {
        buf.skip(getValueLengthV2());
    }
}
