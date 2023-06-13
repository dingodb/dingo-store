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

public class BytesSchema implements DingoSchema<byte[]> {

    private int index;
    private boolean isKey = false;
    private boolean allowNull = true;

    public BytesSchema() {
    }

    public BytesSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.BYTES;
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
        return 0;
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
    public void encodeKey(Buf buf, byte[] data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(5);
                buf.write(NULL);
                buf.reverseWriteInt0();
            } else {
                buf.ensureRemainder(1);
                buf.write(NOTNULL);
                int size = internalEncodeKey(buf, data);
                buf.ensureRemainder(4);
                buf.reverseWriteInt(size);
            }
        } else {
            int size = internalEncodeKey(buf, data);
            buf.ensureRemainder(4);
            buf.reverseWriteInt(size);
        }
    }

    private int internalEncodeKey(Buf buf, byte[] data) {
        int groupNum = data.length / 8;
        int size = (groupNum + 1) * 9;
        int remainderSize = data.length % 8;
        int remaindZero;
        if (remainderSize == 0) {
            remainderSize = 8;
            remaindZero = 8;
        } else {
            remaindZero = 8 - remainderSize;
        }
        buf.ensureRemainder(size);
        for (int i = 0; i < groupNum; i++) {
            buf.write(data, 8 * i, 8);
            buf.write((byte) 255);
        }
        if (remainderSize < 8) {
            buf.write(data, 8 * groupNum, remainderSize);
        }
        for (int i = 0; i < remaindZero; i++) {
            buf.write((byte) 0);
        }
        buf.write((byte) (255 - remaindZero));
        return size;
    }

    @Override
    public void encodeKeyForUpdate(Buf buf, byte[] data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                buf.reverseWriteInt0();
            } else {
                buf.write(NOTNULL);
                buf.reverseWriteInt(internalEncodeKeyForUpdate(buf, data));
            }
        } else {
            buf.reverseWriteInt(internalEncodeKeyForUpdate(buf, data));
        }
    }

    private int internalEncodeKeyForUpdate(Buf buf, byte[] data) {
        int groupNum = data.length / 8;
        int size = (groupNum + 1) * 9;
        int remainderSize = data.length % 8;
        int remaindZero;
        if (remainderSize == 0) {
            remainderSize = 8;
            remaindZero = 8;
        } else {
            remaindZero = 8 - remainderSize;
        }
        int oldSize = buf.reverseReadInt();
        buf.reverseSkip(-4);
        buf.resize(oldSize, size);
        for (int i = 0; i < groupNum; i++) {
            buf.write(data, 8 * i, 8);
            buf.write((byte) 255);
        }
        if (remainderSize < 8) {
            buf.write(data, 8 * groupNum, remainderSize);
        }
        for (int i = 0; i < remaindZero; i++) {
            buf.write((byte) 0);
        }
        buf.write((byte) (255 - remaindZero));
        return size;
    }


    @Override
    public byte[] decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.reverseSkipInt();
                return null;
            }
        }
        return internalReadBytes(buf);
    }

    @Override
    public byte[] decodeKeyPrefix(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }
        return internalReadKeyPrefixBytes(buf);
    }

    private byte[] internalReadKeyPrefixBytes(Buf buf) {
        int length = 0;
        do {
            length += 9;
            buf.skip(8);
        } while(buf.read() == (byte) 255);
        int groupNum = length / 9;
        buf.skip(-1);
        int reminderZero = 255 - buf.read() & 0xFF;
        buf.skip(0 - length);
        int oriLength = groupNum * 8 - reminderZero;
        byte[] data = new byte[oriLength];
        if (oriLength != 0) {
            groupNum --;
            for (int i = 0; i < groupNum; i++) {
                buf.read(data, 8 * i, 8);
                buf.skip(1);
            }
            if (reminderZero != 8) {
                buf.read(data, 8 * groupNum, 8 - reminderZero);
            }
        }
        buf.skip(reminderZero + 1);
        return data;
    }

    private byte[] internalReadBytes(Buf buf) {
        int length = buf.reverseReadInt();
        int groupNum = length / 9;
        buf.skip(length - 1);
        int reminderZero = 255 - buf.read() & 0xFF;
        buf.skip(0 - length);
        int oriLength = groupNum * 8 - reminderZero;
        byte[] data = new byte[oriLength];
        if (oriLength != 0) {
            groupNum --;
            for (int i = 0; i < groupNum; i++) {
                buf.read(data, 8 * i, 8);
                buf.skip(1);
            }
            if (reminderZero != 8) {
                buf.read(data, 8 * groupNum, 8 - reminderZero);
            }
        }
        buf.skip(reminderZero + 1);
        return data;
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skip(buf.reverseReadInt());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, byte[] data) {
        if (allowNull) {
            buf.ensureRemainder(1);
            if (data == null) {
                buf.write(NULL);
            } else {
                buf.write(NOTNULL);
                internalEncodeKey(buf, data);
            }
        } else {
            internalEncodeKey(buf, data);
        }
    }

    @Override
    public void encodeValue(Buf buf, byte[] data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                buf.ensureRemainder(5 + data.length);
                buf.write(NOTNULL);
                buf.writeInt(data.length);
                buf.write(data);
            }
        } else {
            buf.ensureRemainder(4 + data.length);
            buf.writeInt(data.length);
            buf.write(data);
        }
    }

    @Override
    public byte[] decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            } else {
                return buf.read(buf.readInt());
            }
        }
        return buf.read(buf.readInt());
    }

    @Override
    public void skipValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NOTNULL) {
                buf.skip(buf.readInt());
            }
        } else {
            buf.skip(buf.readInt());
        }
    }
}
