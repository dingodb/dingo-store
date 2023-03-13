package io.dingodb.serial.v2.t2;

import java.util.Arrays;

public class Bytes {
    private byte[] bytes;

    public Bytes() {

    }

    public Bytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        return Arrays.equals(bytes, (byte[]) obj);
    }
}
