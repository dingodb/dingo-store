package io.dingodb.serial.v2.t1;

public interface Buf {

    void write(byte b);
    void write(byte[] b);
    void write(byte[] b, int pos, int length);
    void writeInt(int i);
    byte read();
    byte[] read(int length);
    void read(byte[] b, int pos, int length);
    int readInt();
    void reverseWrite(byte b);
    byte reverseRead();
    void reverseWriteInt(int i);
    void reverseWriteInt0();
    int reverseReadInt();
    void skip(int length);
    void reverseSkip(int length);
    void reverseSkipInt();
    void ensureRemainder(int length);
    void resize(int oldSize, int newSize);

    byte[] getBytes();
}
