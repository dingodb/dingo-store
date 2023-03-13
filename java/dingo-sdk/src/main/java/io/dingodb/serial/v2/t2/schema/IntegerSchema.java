package io.dingodb.serial.v2.t2.schema;

import io.dingodb.serial.v1.schema.Type;
import io.dingodb.serial.v2.t2.Buf;
import io.dingodb.serial.v2.t2.ValueBuf;

public class IntegerSchema implements DingoSchema<Integer> {
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
            buf.ensureKeyRemainder(getWithNullTagLength());
            if (data == null) {
                buf.writeKey(NULL);
                internalEncodeKeyNull(buf);
            } else {
                buf.writeKey(NOTNULL);
                internalEncodeKey(buf, data);
            }
        } else {
            buf.ensureKeyRemainder(getDataLength());
            internalEncodeKey(buf, data);
        }
    }

    private void internalEncodeKeyNull(Buf buf) {
        buf.writeKey((byte) 0);
        buf.writeKey((byte) 0);
        buf.writeKey((byte) 0);
        buf.writeKey((byte) 0);
    }

    private void internalEncodeKey(Buf buf, Integer data) {
        buf.writeKey((byte) (data >>> 24 ^ 0x80));
        buf.writeKey((byte) (data >>> 16));
        buf.writeKey((byte) (data >>> 8));
        buf.writeKey((byte) data.intValue());
    }

    @Override
    public Integer decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.readKey() == NULL) {
                buf.skipKey(getDataLength());
                return null;
            }
        }
        return (((buf.readKey() & 0xFF ^ 0x80) << 24)
                | ((buf.readKey() & 0xFF) << 16)
                | ((buf.readKey() & 0xFF) << 8)
                | buf.readKey() & 0xFF);
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skipKey(getLength());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Integer data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Integer data) {
        if (allowNull) {
            buf.ensureValueRemainder(getWithNullTagLength(), 0);
            if (data == null) {
                buf.writeValue(NULL, 0);
                internalEncodeValueNull(buf);
            } else {
                buf.writeValue(NOTNULL, 0);
                internalEncodeValue(buf, data);
            }
        } else {
            buf.ensureValueRemainder(getDataLength(), 0);
            internalEncodeValue(buf, data);
        }
    }

    @Override
    public void encodeValue(ValueBuf buf, Integer data) {
        if (allowNull) {
            if (data == null) {
                buf.write(NULL);
                internalEncodeValueNull(buf);
            } else {
                buf.write(NOTNULL);
                internalEncodeValue(buf, data);
            }
        } else {
            internalEncodeValue(buf, data);
        }
    }

    private void internalEncodeValueNull(Buf buf) {
        buf.writeValue(0, (byte) 0, (byte) 0, (byte) 0, (byte) 0);
    }

    private void internalEncodeValueNull(ValueBuf buf) {
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
    }

    private void internalEncodeValue(Buf buf, Integer data) {
        buf.writeValue(0, (byte) (data >>> 24), (byte) (data >>> 16), (byte) (data >>> 8), (byte) data.intValue());
    }

    private void internalEncodeValue(ValueBuf buf, Integer data) {
        buf.write((byte) (data >>> 24));
        buf.write((byte) (data >>> 16));
        buf.write((byte) (data >>> 8));
        buf.write((byte) data.intValue());
    }

    @Override
    public Integer decodeValue(ValueBuf buf) {
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
    public void skipValue(ValueBuf buf) {
        buf.skip(getLength());
    }
}
