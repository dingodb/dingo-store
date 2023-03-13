package io.dingodb.serial.v2.t2.test;

import java.util.HashMap;
import java.util.Map;

public class BytesMapTest {
    public static void main(String[] args) {
        Map<byte[], byte[]> store = new HashMap<>();

        store.put(new byte[] {1,2,3}, new byte[]{3,2,1});
        store.put(new byte[] {1,2,4}, new byte[]{4,2,1});
        store.put(new byte[] {5,6,7}, new byte[]{7,6,5});

        byte[] a = store.get(new byte[] {1,2,4});
        System.out.println(a);



    }
}
