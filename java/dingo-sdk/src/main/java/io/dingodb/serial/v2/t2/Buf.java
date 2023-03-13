package io.dingodb.serial.v2.t2;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t1.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Buf {
    private byte[] buf;
    private Map<Integer, byte[]> data = new HashMap<Integer, byte[]>();
    private int[] dataPos;

    private int forwardPos;
    private int reversePos;

    public Buf(int bufSize) {
        this.buf = new byte[bufSize];
        forwardPos = 0;
        reversePos = buf.length - 1;
    }

    public Buf(byte[] buf) {
        this.buf = buf;
        forwardPos = 0;
        reversePos = buf.length - 1;
    }

    public Buf(int bufSize, Map<Integer, Integer> valueSize) {
        this.buf = new byte[bufSize];
        forwardPos = 0;
        reversePos = buf.length - 1;
        int maxIndex = 0;
        for (Integer index : valueSize.keySet()) {
            if (index > maxIndex) {
                maxIndex = index;
            }
        }
        dataPos = new int[maxIndex+1];
        for(Map.Entry<Integer, Integer> value : valueSize.entrySet()) {
            data.put(value.getKey(), new byte[value.getValue()]);
        }
    }

    public void writeKey(byte b) {
        buf[forwardPos++] = b;
    }

    public void writeValue(byte b, int index) {
        data.get(index)[dataPos[index]++] = b;
        //dataPos.put(index, dataPos.get(index)+1);
    }

    public void writeValue(int index, byte... bs) {
        //int valuePos = dataPos.get(index);
        byte[] value = data.get(index);
        for (byte b : bs) {
            value[dataPos[index]++] = b;
        }
        //dataPos.put(index, valuePos);
    }

    public void writeKey(byte[] b) {
        System.arraycopy(b, 0, buf, forwardPos, b.length);
        forwardPos += b.length;
    }

    public void writeKey(byte[] b, int pos, int length) {
        System.arraycopy(b, pos, buf, forwardPos, length);
        forwardPos += length;
    }

    public void writeKeyInt(int i) {
        buf[forwardPos++] = (byte) (i >>> 24);
        buf[forwardPos++] = (byte) (i >>> 16);
        buf[forwardPos++] = (byte) (i >>> 8);
        buf[forwardPos++] = (byte) i;
    }

    public void writeValueInt(int i, int index) {
        byte[] value = data.get(index);
        value[dataPos[index]++] = (byte) (i >>> 24);
        value[dataPos[index]++] = (byte) (i >>> 16);
        value[dataPos[index]++] = (byte) (i >>> 8);
        value[dataPos[index]++] = (byte) i;
    }

    public byte readKey() {
        return buf[forwardPos++];
    }

    public byte readValue(int index) {
        return data.get(index)[dataPos[index]++];
    }

    public byte[] readKey(int length) {
        byte[] b = new byte[length];
        System.arraycopy(buf, forwardPos, b, 0, length);
        forwardPos += length;
        return b;
    }

    public void readKey(byte[] b, int pos, int length) {
        System.arraycopy(buf, forwardPos, b, pos, length);
        forwardPos += length;
    }

    public int readKeyInt() {
        return (((buf[forwardPos++] & 0xFF) << 24)
                | ((buf[forwardPos++] & 0xFF) << 16)
                | ((buf[forwardPos++] & 0xFF) << 8)
                | buf[forwardPos++] & 0xFF);
    }

    public void reverseKeyWrite(byte b) {
        buf[reversePos--] = b;
    }

    public byte reverseKeyRead() {
        return buf[reversePos--];
    }

    public void reverseKeyWriteInt(int i) {
        buf[reversePos--] = (byte) (i >>> 24);
        buf[reversePos--] = (byte) (i >>> 16);
        buf[reversePos--] = (byte) (i >>> 8);
        buf[reversePos--] = (byte) i;
    }

    public void reverseKeyWriteInt0() {
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
    }

    public int reverseKeyReadInt() {
        return (((buf[reversePos--] & 0xFF) << 24)
                | ((buf[reversePos--] & 0xFF) << 16)
                | ((buf[reversePos--] & 0xFF) << 8)
                | buf[reversePos--] & 0xFF);
    }

    public void skipKey(int length) {
        forwardPos += length;
    }

    public void skipValue(int length, int index) {
        dataPos[index] += length;
    }

    public void reverseKeySkip(int length) {
        reversePos -= length;
    }

    public void reverseKeySkipInt() {
        reversePos -= 4;
    }

    public void ensureKeyRemainder(int length) {
        if ((forwardPos + length - 1) > reversePos) {
            int newSize;
            if (length > Config.SCALE) {
                newSize = buf.length + length;
            } else {
                newSize = buf.length + Config.SCALE;
            }

            byte[] newBuf = new byte[newSize];
            System.arraycopy(buf, 0, newBuf, 0, forwardPos);
            int reverseSize = buf.length - reversePos - 1;
            System.arraycopy(buf, reversePos + 1, newBuf, newSize - reverseSize, reverseSize);
            reversePos = newSize - reverseSize - 1;
            buf = newBuf;
        }
    }

    public void ensureValueRemainder(int length, int index) {
        byte[] value = data.get(index);
        if (dataPos[index] + length > value.length) {
            byte[] newValue = new byte[dataPos[index] + length];
            System.arraycopy(value, 0, newValue, 0, dataPos[index]);
            data.put(index, newValue);
        }
    }

    public byte[] getBytes() {
        ensureKeyRemainder(4);
        int emptySize = reversePos - forwardPos + 1;
        if (emptySize == 4) {
            return buf;
        }
        if (emptySize > 0) {
            int finalSize = buf.length - emptySize;
            byte[] finalBuf = new byte[finalSize];
            System.arraycopy(buf, 0, finalBuf, 0, forwardPos);
            System.arraycopy(buf, reversePos + 1, finalBuf, forwardPos, finalSize - forwardPos);
            buf = finalBuf;
            reversePos = forwardPos - 1;
            return buf;
        }
        if (emptySize < 0) {
            throw new RuntimeException("Wrong Key Buf");
        }
        return null;
    }

    public List<KeyValue> getData() {
        ensureKeyRemainder(4);
        List<KeyValue> kvs = new ArrayList<>();
        int finalSize = buf.length - reversePos + forwardPos + 3;
        for (Integer index : data.keySet()) {
            writeKeyInt(index);
            byte[] indexKey = new byte[finalSize];
            System.arraycopy(buf, 0, indexKey, 0, forwardPos);
            System.arraycopy(buf, reversePos + 1, indexKey, forwardPos, finalSize - forwardPos);
            KeyValue kv = new KeyValue(null, null);
            kv.setKey(indexKey);
            forwardPos -= 4;
            byte[] value = data.get(index);
            byte[] finalValue = new byte[dataPos[index]];
            System.arraycopy(value, 0, finalValue, 0, dataPos[index]);
            kv.setValue(finalValue);
            kvs.add(kv);
        }
        return kvs;
    }

    public List<byte[]> getKeyData() {
        ensureKeyRemainder(4);
        List<byte[]> keyData = new ArrayList<>();
        int finalSize = buf.length - reversePos + forwardPos + 3;
        for (Integer index : data.keySet()) {
            writeKeyInt(index);
            byte[] indexKey = new byte[finalSize];
            System.arraycopy(buf, 0, indexKey, 0, forwardPos);
            System.arraycopy(buf, reversePos + 1, indexKey, forwardPos, finalSize - forwardPos);
            keyData.add(indexKey);
            forwardPos -= 4;
        }
        return keyData;
    }
}
