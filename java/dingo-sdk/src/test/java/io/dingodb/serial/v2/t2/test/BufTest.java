package io.dingodb.serial.v2.t2.test;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t2.Buf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufTest {
    public static void main(String[] args) {

        Map<Integer, Integer> valueSize = new HashMap<>();
        valueSize.put(0,100);
        valueSize.put(1,100);
        valueSize.put(2,100);
        Buf buf = new Buf(15, valueSize);
        buf.writeKeyInt(1);
        buf.writeKey((byte) 2);
        buf.reverseKeyWriteInt(0);
        buf.writeValueInt(0, 0);
        buf.writeValueInt(1, 1);
        buf.writeValueInt(2, 2);

        buf.writeValue(0, (byte) 1, (byte) 2, (byte) 5);
        byte[] key = buf.getBytes();
        System.out.println(key);
        List<KeyValue> keys = buf.getData();
        System.out.println(keys);




    }



}
