package io.dingodb.serial.v2.t2;

public class ValueBuf {
    private byte[] buf;
    private int forwardPos = 0;

    public ValueBuf(byte[] buf) {
        this.buf = buf;
    }

    public byte read() {
        return buf[forwardPos++];
    }

    public void write(byte b) {
        buf[forwardPos++] = b;
    }

    public byte[] read(int length) {
        byte[] b = new byte[length];
        System.arraycopy(buf, forwardPos, b, 0, length);
        forwardPos += length;
        return b;
    }

    public int readInt() {
        return (((buf[forwardPos++] & 0xFF) << 24)
                | ((buf[forwardPos++] & 0xFF) << 16)
                | ((buf[forwardPos++] & 0xFF) << 8)
                | (buf[forwardPos++] & 0xFF));
    }

    public void write(byte[] bs) {
        System.arraycopy(bs, 0, buf, forwardPos, bs.length);
        forwardPos += bs.length;
    }

    public byte[] readRemainder() {
        int remainderLength = buf.length - forwardPos;
        byte[] b = new byte[remainderLength];
        System.arraycopy(buf, forwardPos, b, 0, remainderLength);
        return b;
    }

    public void skip(int length) {
        forwardPos += length;
    }

    public void ensureRemainder(int size) {
        if (forwardPos + size > buf.length) {
            byte[] newBuf = new byte[forwardPos + size];
            System.arraycopy(buf, 0, newBuf, 0, forwardPos);
            buf = newBuf;
        }
    }

    public byte[] getBytes() {
        if (forwardPos == buf.length) {
            return buf;
        } else {
            byte[] finalBuf = new byte[forwardPos];
            System.arraycopy(buf, 0, finalBuf, 0, forwardPos);
            return finalBuf;
        }
    }
}
