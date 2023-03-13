package io.dingodb.serial.base.common;

public class BytesTest {

    private static byte[] b0 = new byte[] {0,0,0,0};

    public static void main(String[] args) {

        byte[] bf = new byte[10];

        long l1 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            write(bf, 0);
        }
        long l2 = System.currentTimeMillis();

        long l3 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            //byte[] b00 = get0Bytes();
            bf[0] = b0[0];
            bf[1] = b0[1];
            bf[2] = b0[2];
            bf[3] = b0[3];
        }
        long l4 = System.currentTimeMillis();

        long l5 = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            //byte[] b00 = get0Bytes();
            System.arraycopy(b0, 0, bf, 0, 4);
        }
        long l6 = System.currentTimeMillis();

        System.out.println("1: " + (l2 - l1));
        System.out.println("2: " + (l4 - l3));
        System.out.println("3: " + (l6 - l5));
    }



    private static byte[] get0Bytes() {
        return b0;
    }

    private static int write(byte[] b, int index) {
        b[index++] =  43545 >>> 24;
        b[index++] =  2344465 >>> 16;
        b[index++] =  23432 >>> 8;
        b[index++] =  (byte)4665;
        return index;
    }
}
