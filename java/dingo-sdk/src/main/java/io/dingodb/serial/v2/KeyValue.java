package io.dingodb.serial.v2;

public class KeyValue {
    private byte[] key;
    private byte[] value;

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
