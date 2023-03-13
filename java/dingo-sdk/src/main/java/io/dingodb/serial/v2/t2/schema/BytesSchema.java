package io.dingodb.serial.v2.t2.schema;

import io.dingodb.serial.v1.schema.Type;
import io.dingodb.serial.v2.t2.Buf;
import io.dingodb.serial.v2.t2.ValueBuf;

public class BytesSchema implements DingoSchema<byte[]> {

    private int index;
    private boolean isKey;
    private boolean allowNull;

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
            buf.ensureKeyRemainder(5);
            if (data == null) {
                buf.writeKey(NULL);
                buf.reverseKeyWriteInt0();
            } else {
                buf.writeKey(NOTNULL);
                buf.reverseKeyWriteInt(internalEncodeKey(buf, data));
            }
        } else {
            buf.ensureKeyRemainder(4);
            buf.reverseKeyWriteInt(internalEncodeKey(buf, data));
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
        buf.ensureKeyRemainder(size);
        for (int i = 0; i < groupNum; i++) {
            buf.writeKey(data, 8 * i, 8);
            buf.writeKey((byte) 255);
        }
        if (remainderSize < 8) {
            buf.writeKey(data, 8 * groupNum, remainderSize);
        }
        for (int i = 0; i < remaindZero; i++) {
            buf.writeKey((byte) 0);
        }
        buf.writeKey((byte) (255 - remaindZero));
        return size;
    }


    @Override
    public byte[] decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.readKey() == NULL) {
                buf.reverseKeySkipInt();
                return null;
            }
        }
        return internalReadBytes(buf);
    }

    private byte[] internalReadBytes(Buf buf) {
        int length = buf.reverseKeyReadInt();
        int groupNum = length / 9;
        buf.skipKey(length - 1);
        int reminderZero = 255 - buf.readKey() & 0xFF;
        buf.skipKey(0 - length);
        int oriLength = groupNum * 8 - reminderZero;
        byte[] data = new byte[oriLength];
        if (oriLength != 0) {
            groupNum --;
            for (int i = 0; i < groupNum; i++) {
                buf.readKey(data, 8 * i, 8);
                buf.skipKey(1);
            }
            if (reminderZero != 8) {
                buf.readKey(data, 8 * groupNum, 8 - reminderZero);
            }
        }
        buf.skipKey(reminderZero + 1);
        return data;
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skipKey(buf.reverseKeyReadInt());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, byte[] data) {
        if (allowNull) {
            buf.ensureKeyRemainder(1);
            if (data == null) {
                buf.writeKey(NULL);
            } else {
                buf.writeKey(NOTNULL);
                internalEncodeKey(buf, data);
            }
        } else {
            internalEncodeKey(buf, data);
        }
    }

    @Override
    public void encodeValue(Buf buf, byte[] data) {
        if (allowNull) {
            buf.ensureValueRemainder(1, index);
            if (data == null) {
                buf.writeValue(NULL, index);
            } else {
                buf.writeValue(NOTNULL, index);
                buf.ensureValueRemainder(data.length, index);
                buf.writeValue(index, data);
            }
        } else {
            buf.ensureValueRemainder(data.length, index);
            buf.writeValue(index, data);
        }
    }

    @Override
    public void encodeValue(ValueBuf buf, byte[] data) {
        if (allowNull) {
            buf.ensureRemainder(1);
            if (data == null) {
                buf.write(NULL);
            } else {
                buf.write(NOTNULL);
                buf.ensureRemainder(data.length);
                buf.write(data);
            }
        } else {
            buf.ensureRemainder(data.length);
            buf.write(data);
        }
    }

    @Override
    public byte[] decodeValue(ValueBuf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                return null;
            }
        }
        return buf.readRemainder();
    }

    @Override
    public void skipValue(ValueBuf buf) {
    }
}
