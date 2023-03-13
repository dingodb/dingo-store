package io.dingodb.serial.v2.t2.schema;

import io.dingodb.serial.v1.schema.Type;
import io.dingodb.serial.v2.t2.Buf;
import io.dingodb.serial.v2.t2.ValueBuf;

public class BooleanSchema implements DingoSchema<Boolean> {
    private int index;
    private boolean isKey;
    private boolean allowNull = true;

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
            buf.ensureKeyRemainder(getWithNullTagLength());
            if (data == null) {
                buf.writeKey(NULL);
                buf.writeKey((byte) 0);
            } else {
                buf.writeKey(NOTNULL);
                buf.writeKey(data ? (byte) 1 : (byte) 0);
            }
        } else {
            buf.ensureKeyRemainder(getDataLength());
            buf.writeKey(data ? (byte) 1 : (byte) 0);
        }
    }

    @Override
    public Boolean decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.readKey() == NULL) {
                buf.skipKey(getDataLength());
                return null;
            }
        }
        return buf.readKey() == (byte) 0 ? false : true;
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skipKey(getLength());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Boolean data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Boolean data) {
        if (allowNull) {
            buf.ensureValueRemainder(getWithNullTagLength(), 0);
            if (data == null) {
                buf.writeValue(NULL, 0);
                buf.writeValue(0, (byte) 0);
            } else {
                buf.writeValue(NOTNULL, 0);
                buf.writeValue(0, data ? (byte) 1 : (byte) 0);
            }
        } else {
            buf.ensureValueRemainder(getDataLength(), 0);
            buf.writeValue(0, data ? (byte) 1 : (byte) 0);
        }
    }

    @Override
    public void encodeValue(ValueBuf buf, Boolean data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                buf.write((byte) 0);
            } else {
                buf.write(NOTNULL);
                buf.write(data ? (byte) 1 : (byte) 0);
            }
        } else {
            buf.write(data ? (byte) 1 : (byte) 0);
        }
    }
    @Override
    public Boolean decodeValue(ValueBuf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        return buf.read() == (byte) 0 ? false : true;
    }

    @Override
    public void skipValue(ValueBuf buf) {
        buf.skip(getLength());
    }
}
