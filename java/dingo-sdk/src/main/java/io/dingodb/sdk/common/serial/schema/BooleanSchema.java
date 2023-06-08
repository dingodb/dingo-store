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

public class BooleanSchema implements DingoSchema<Boolean> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

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

    private int getWithNullTagLength() {
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

    private void internalEncodeNull(Buf buf) {
        buf.write((byte) 0);
    }

    private void internalEncodeData(Buf buf, Boolean b) {
        if (b) {
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
    public void skipValue(Buf buf) {
        buf.skip(getLength());
    }
}
