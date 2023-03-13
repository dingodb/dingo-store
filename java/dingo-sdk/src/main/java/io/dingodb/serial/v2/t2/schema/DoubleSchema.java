package io.dingodb.serial.v2.t2.schema;

import io.dingodb.serial.v1.schema.Type;
import io.dingodb.serial.v2.t2.Buf;
import io.dingodb.serial.v2.t2.ValueBuf;

public class DoubleSchema implements DingoSchema<Double> {
    private int index;
    private boolean isKey;
    private boolean allowNull = true;

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
        buf.writeKey((byte) 0);
        buf.writeKey((byte) 0);
        buf.writeKey((byte) 0);
        buf.writeKey((byte) 0);
    }

    private void internalEncodeKey(Buf buf, Double data) {
        long ln = (Double.doubleToLongBits(data));
        if (data >= 0) {
            buf.writeKey((byte) (ln >>> 56 ^ 0x80));
            buf.writeKey((byte) (ln >>> 48));
            buf.writeKey((byte) (ln >>> 40));
            buf.writeKey((byte) (ln >>> 32));
            buf.writeKey((byte) (ln >>> 24));
            buf.writeKey((byte) (ln >>> 16));
            buf.writeKey((byte) (ln >>> 8));
            buf.writeKey((byte) ln);
        } else {
            buf.writeKey((byte) (~ ln >>> 56));
            buf.writeKey((byte) (~ ln >>> 48));
            buf.writeKey((byte) (~ ln >>> 40));
            buf.writeKey((byte) (~ ln >>> 32));
            buf.writeKey((byte) (~ ln >>> 24));
            buf.writeKey((byte) (~ ln >>> 16));
            buf.writeKey((byte) (~ ln >>> 8));
            buf.writeKey((byte) ~ ln);
        }
    }

    @Override
    public Double decodeKey(Buf buf) {
        if (allowNull) {
            if (buf.readKey() == NULL) {
                buf.skipKey(getDataLength());
                return null;
            }
        }
        long l = 0;
        if ((buf.readKey() & 0xFF) >= 0x80) {
            l |= buf.readKey() & 0xFF ^ 0x80;
            for (int i = 0; i < 7; i++) {
                l <<= 8;
                l |= buf.readKey() & 0xFF;
            }
        } else {
            for (int i = 0; i < 8; i++) {
                l <<= 8;
                l |= ~buf.readKey() & 0xFF;
            }
        }
        return Double.longBitsToDouble(l);
    }

    @Override
    public void skipKey(Buf buf) {
        buf.skipKey(getLength());
    }

    @Override
    public void encodeKeyPrefix(Buf buf, Double data) {
        encodeKey(buf, data);
    }

    @Override
    public void encodeValue(Buf buf, Double data) {
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
    public void encodeValue(ValueBuf buf, Double data) {
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
        buf.writeValue(0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0, (byte) 0);
    }

    private void internalEncodeValueNull(ValueBuf buf) {
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
        buf.write((byte) 0);
    }

    private void internalEncodeValue(Buf buf, Double data) {
        long ln = (Double.doubleToLongBits(data));
        buf.writeValue(0, (byte) (ln >>> 56), (byte) (ln >>> 48), (byte) (ln >>> 40), (byte) (ln >>> 32),
                (byte) (ln >>> 24), (byte) (ln >>> 16), (byte) (ln >>> 8), (byte) ln);
    }

    private void internalEncodeValue(ValueBuf buf, Double data) {
        long ln = (Double.doubleToLongBits(data));
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
    public Double decodeValue(ValueBuf buf) {
        if (allowNull) {
            if (buf.read() == NULL) {
                buf.skip(getDataLength());
                return null;
            }
        }
        long l = 0;
        for (int i = 0; i < 8; i++) {
            l <<= 8;
            l |= buf.read() & 0xFF;
        }
        return Double.longBitsToDouble(l);
    }

    @Override
    public void skipValue(ValueBuf buf) {
        buf.skip(getLength());
    }
}
