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

import java.nio.charset.StandardCharsets;

public class StringSchema implements DingoSchema<String> {

    private int index;
    private boolean isKey;
    private boolean allowNull = true;

    public StringSchema() {
    }

    public StringSchema(int index) {
        this.index = index;
    }

    @Override
    public Type getType() {
        return Type.STRING;
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
    public int getWithNullTagLength() {
        return 1;
    }

    @Override
    public int getValueLengthV2() {
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

    public void encodeKey(Buf buf, String data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(5);
                buf.write(NULL);
                buf.reverseWriteInt0();
            } else {
                buf.ensureRemainder(1);
                buf.write(NOTNULL);
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                int size = internalEncodeKey(buf, bytes);
                buf.ensureRemainder(4);
                buf.reverseWriteInt(size);
            }
        } else {
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            int size = internalEncodeKey(buf, bytes);
            buf.ensureRemainder(4);
            buf.reverseWriteInt(size);
        }
    }

    public void encodeKeyV2(Buf buf, String data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
                //buf.reverseWriteInt0();
            } else {
                buf.ensureRemainder(1);
                buf.write(NOTNULL);
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                int size = internalEncodeKey(buf, bytes);
                //buf.ensureRemainder(4);
                //buf.reverseWriteInt(size);
            }
        } else {
            if (data == null) {
                throw new RuntimeException("Data is not allow as null.");
            }
            //buf.ensureRemainder(1);
            //buf.write(NOTNULL);
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            int size = internalEncodeKey(buf, bytes);
            //buf.ensureRemainder(4);
            //buf.reverseWriteInt(size);
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
    public void encodeKeyForUpdate(Buf buf, String data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                buf.reverseWriteInt0();
            } else {
                buf.write(NOTNULL);
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                buf.reverseWriteInt(internalEncodeKeyForUpdate(buf, bytes));
            }
        } else {
            buf.write(NOTNULL);
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            buf.reverseWriteInt(internalEncodeKeyForUpdate(buf, bytes));
        }
    }

    @Override
    public void encodeKeyForUpdateV2(Buf buf, String data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                //buf.reverseWriteInt0();
            } else {
                buf.write(NOTNULL);
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                //buf.reverseWriteInt(internalEncodeKeyForUpdate(buf, bytes));
                internalEncodeKeyForUpdate(buf, bytes);
            }
        } else {
            if (data == null) {
                throw new RuntimeException("Data is not allow as null.");
            }
            buf.write(NOTNULL);
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            //buf.reverseWriteInt(internalEncodeKeyForUpdate(buf, bytes));
            internalEncodeKeyForUpdate(buf, bytes);
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
    public String decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.reverseSkipInt();
                return null;
            }
        }
        return new String(internalReadBytes(buf), StandardCharsets.UTF_8);
    }

    @Override
    public String decodeKeyV2(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                //buf.reverseSkipInt();
                return null;
            }
        }

        return new String(internalReadBytesV2(buf), StandardCharsets.UTF_8);
    }

    //This interface is both used by v1 and v2. We use same way to decode key prefix.
    //In the new way, we decode string value directly but not by length field.
    @Override
    public String decodeKeyPrefix(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }
        return new String(internalReadKeyPrefixBytes(buf), StandardCharsets.UTF_8);
    }

    private byte[] internalReadKeyPrefixBytes(Buf buf) {
        int length = 0;
        do {
            length += 9;
            buf.skip(8);
        }
        while (buf.read() == (byte) 255);
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

    private byte[] internalReadBytesV2(Buf buf) {
        return internalReadKeyPrefixBytes(buf);
    }

    @Override
    public void skipKey(Buf buf) {
        if (allowNull) {
            buf.skip(buf.reverseReadInt() + 1);
        } else {
            buf.skip(buf.reverseReadInt());
        }
    }

    @Override
    public void skipKeyV2(Buf buf) {
        if (allowNull) {
            //buf.skip(buf.reverseReadInt() + 1);
            buf.skip(1);
        }

        internalReadBytesV2(buf);
    }

    @Override
    public void encodeKeyPrefix(Buf buf, String data) {
        if (allowNull) {
            buf.ensureRemainder(1);
            if (data == null) {
                buf.write(NULL);
            } else {
                buf.write(NOTNULL);
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                internalEncodeKey(buf, bytes);
            }
        } else {
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            internalEncodeKey(buf, bytes);
        }
    }

    @Override
    public void encodeValue(Buf buf, String data) {
        if (allowNull) {
            if (data == null) {
                buf.ensureRemainder(1);
                buf.write(NULL);
            } else {
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                buf.ensureRemainder(1 + 4 + bytes.length);
                buf.write(NOTNULL);
                buf.writeInt(bytes.length);
                buf.write(bytes);
            }
        } else {
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            buf.ensureRemainder(4 + bytes.length);
            buf.writeInt(bytes.length);
            buf.write(bytes);
        }
    }

    @Override
    public int encodeValueV2(Buf buf, String data) {
        int len = 0;

        if (allowNull) {
            if (data == null) {
                return 0;
            } else {
                byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
                buf.ensureRemainder(4 + bytes.length);
                buf.writeInt(bytes.length);
                buf.write(bytes);

                len += 4 + bytes.length;
            }
        } else {
            byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
            buf.ensureRemainder(4 + bytes.length);
            buf.writeInt(bytes.length);
            buf.write(bytes);

            len += 4 + bytes.length;
        }

        return len;
    }

    @Override
    public String decodeValue(Buf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            } else {
                return new String(buf.read(buf.readInt()), StandardCharsets.UTF_8);
            }
        }
        return new String(buf.read(buf.readInt()), StandardCharsets.UTF_8);
    }

    @Override
    public String decodeValueV2(Buf buf) {
        return new String(buf.read(buf.readInt()), StandardCharsets.UTF_8);
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

    @Override
    public void skipValueV2(Buf buf) {
        buf.skip(buf.readInt());
    }
}
