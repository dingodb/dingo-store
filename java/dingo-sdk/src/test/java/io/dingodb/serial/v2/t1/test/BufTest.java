package io.dingodb.serial.v2.t1.test;

import io.dingodb.serial.v2.t1.Buf;
import io.dingodb.serial.v2.t1.BufImpl;

public class BufTest {
    public static void main(String[] args) {

        testEnsureRemainder();

    }


    public static void testGetBytes() {
        Buf buf = new BufImpl(new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14});

        buf.skip(4);
        buf.reverseSkip(4);

        System.out.println(buf.getBytes());

        System.out.println(buf.getBytes());

        System.out.println(1);
    }

    public static void testResize() {
        Buf buf = new BufImpl(new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14});

        buf.skip(4);
        buf.reverseSkip(4);

        buf.resize(2, 5);

        System.out.println(buf);
    }

    public static void testEnsureRemainder() {
        Buf buf = new BufImpl(new byte[] {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14});

        buf.skip(4);
        buf.reverseSkip(4);

        buf.ensureRemainder(2);
        buf.ensureRemainder(7);
        buf.ensureRemainder(10);

        System.out.println(buf);
    }
}
