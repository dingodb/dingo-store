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

public class BooleanSchema implements DingoSchema<Boolean> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    @Getter
    @Setter
    private long precision;

    @Getter
    @Setter
    private long scale;

    public BooleanSchema() {
    }

    public BooleanSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.BOOLEAN;
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
        return 2;
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

    @Override
    public void encodeKey(Buf buf, Boolean data) {
        if (allowNull) {
            buf.ensureRemainder(getWithNullTagLength());
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            buf.ensureRemainder(getDataLength());
            internalEncodeData(buf, data);
        }
    }

    public void encodeKeyV2(Buf buf, Boolean data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, Boolean data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            internalEncodeData(buf, data);
        }
    }

    @Override
    public void encodeKeyForUpdateV2(Buf buf, Boolean data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            if (data == null) {
                throw new RuntimeException("Data is not allow as null.");
            }
            buf.write(NOTNULL);
            internalEncodeData(buf, data);
        }
    }

    private void internalEncodeNull(Buf buf) {
        buf.write((byte) 0);
    }

    private void internalEncodeData(Buf buf, Boolean boolVal) {
        if (boolVal) {
            buf.write((byte) 1);
        } else {
            buf.write((byte) 0);
        }
    }

    @Override
    public Boolean decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return internalDecodeData(buf);
    }

    public Boolean decodeKeyV2(Buf buf) {
        return decodeKey(buf);
    }

    @Override
    public Boolean decodeKeyPrefix(Buf buf) {
        return decodeKey(buf);
    }


    private Boolean internalDecodeData(Buf buf) {
        return buf.read() == (byte) 0 ? false : true;
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skip(getLength());
    }

    public void skipKeyV2(Buf buf) {
        skipKey(buf);
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Boolean data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Boolean data) {
        if (allowNull) {
            buf.ensureRemainder(getWithNullTagLength());
            if (data == null) {
                buf.write(NULL);
                internalEncodeNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeData(buf, data);
            }
        } else {
            buf.ensureRemainder(getDataLength());
            internalEncodeData(buf, data);
        }
    }

    @Override
    public int encodeValueV2(Buf buf, Boolean data) {
        int len = getValueLengthV2();
        buf.ensureRemainder(len);
        if (allowNull) {
            if (data == null) {
                return 0;
            } else {
                internalEncodeData(buf, data);
            }
        } else {
            internalEncodeData(buf, data);
        }

        return len;
    }

    @Override
    public Boolean decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return internalDecodeData(buf);
    }

    @Override
    public Boolean decodeValueV2(Buf buf) {
        return internalDecodeData(buf);
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
