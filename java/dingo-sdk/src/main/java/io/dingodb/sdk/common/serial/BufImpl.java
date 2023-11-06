/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.common.serial;

public class BufImpl implements Buf {

    private byte[] buf;
    private int forwardPos;
    private int reversePos;

    public BufImpl(int bufSize) {
        this.buf = new byte[bufSize];
        this.forwardPos = 0;
        this.reversePos = bufSize - 1;
    }

    public BufImpl(byte[] keyBuf) {
        this.buf = keyBuf;
        this.forwardPos = 0;
        this.reversePos = keyBuf.length - 1;
    }

    @Override
    public void write(byte b) {
        buf[forwardPos++] = b;
    }

    @Override
    public void write(byte[] b) {
        System.arraycopy(b, 0, buf, forwardPos, b.length);
        forwardPos += b.length;
    }

    @Override
    public void write(byte[] b, int pos, int length) {
        System.arraycopy(b, pos, buf, forwardPos, length);
        forwardPos += length;
    }

    @Override
    public void writeInt(int i) {
        buf[forwardPos++] = (byte) (i >>> 24);
        buf[forwardPos++] = (byte) (i >>> 16);
        buf[forwardPos++] = (byte) (i >>> 8);
        buf[forwardPos++] = (byte) i;
    }

    @Override
    public void writeLong(long l) {
        buf[forwardPos++] = (byte) (l >>> 56);
        buf[forwardPos++] = (byte) (l >>> 48);
        buf[forwardPos++] = (byte) (l >>> 40);
        buf[forwardPos++] = (byte) (l >>> 32);
        buf[forwardPos++] = (byte) (l >>> 24);
        buf[forwardPos++] = (byte) (l >>> 16);
        buf[forwardPos++] = (byte) (l >>> 8);
        buf[forwardPos++] = (byte) l;
    }

    @Override
    public byte peek() {
        return buf[forwardPos];
    }

    @Override
    public int peekInt() {
        return (
            ((buf[forwardPos]     & 0xFF) << 24)
          | ((buf[forwardPos + 1] & 0xFF) << 16)
          | ((buf[forwardPos + 2] & 0xFF) << 8 )
          | ( buf[forwardPos + 3] & 0xFF       )
        );
    }

    @Override
    public long peekLong() {
        long l = buf[forwardPos] & 0xFF;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf[forwardPos + i + 1] & 0xFF;
        }
        return l;
    }

    @Override
    public byte read() {
        return buf[forwardPos++];
    }

    @Override
    public byte[] read(int length) {
        byte[] b = new byte[length];
        System.arraycopy(buf, forwardPos, b, 0, length);
        forwardPos += length;
        return b;
    }

    @Override
    public void read(byte[] b, int pos, int length) {
        System.arraycopy(buf, forwardPos, b, pos, length);
        forwardPos += length;
    }

    @Override
    public int readInt() {
        return (((buf[forwardPos++] & 0xFF) << 24)
                | ((buf[forwardPos++] & 0xFF) << 16)
                | ((buf[forwardPos++] & 0xFF) << 8)
                | buf[forwardPos++] & 0xFF);
    }

    @Override
    public long readLong() {
        long l = buf[forwardPos++]  & 0xFF;
        for (int i = 0; i < 7; i++) {
            l <<= 8;
            l |= buf[forwardPos++] & 0xFF;
        }
        return l;
    }

    @Override
    public void reverseWrite(byte b) {
        buf[reversePos--] = b;
    }

    @Override
    public byte reverseRead() {
        return buf[reversePos--];
    }

    @Override
    public void reverseWriteInt(int i) {
        buf[reversePos--] = (byte) (i >>> 24);
        buf[reversePos--] = (byte) (i >>> 16);
        buf[reversePos--] = (byte) (i >>> 8);
        buf[reversePos--] = (byte) i;
    }

    @Override
    public void reverseWriteInt0() {
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
        buf[reversePos--] = (byte) 0;
    }

    @Override
    public int reverseReadInt() {
        return (((buf[reversePos--] & 0xFF) << 24)
                | ((buf[reversePos--] & 0xFF) << 16)
                | ((buf[reversePos--] & 0xFF) << 8)
                | buf[reversePos--] & 0xFF);
    }

    @Override
    public void skip(int length) {
        forwardPos += length;
    }

    @Override
    public void reverseSkip(int length) {
        reversePos -= length;
    }

    @Override
    public void reverseSkipInt() {
        reversePos -= 4;
    }

    @Override
    public void ensureRemainder(int length) {
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

    @Override
    public void resize(int oldSize, int newSize) {
        if (oldSize != newSize) {
            byte[] newBuf = new byte[buf.length + newSize - oldSize];
            System.arraycopy(buf, 0, newBuf, 0, forwardPos);
            int backPos = forwardPos + oldSize;
            System.arraycopy(buf, backPos, newBuf, forwardPos + newSize, buf.length - backPos);
            buf = newBuf;
            reversePos += (newSize - oldSize);
        }
    }

    @Override
    public boolean isEnd() {
        return (reversePos - forwardPos + 1) == 0;
    }

    @Override
    public byte[] getBytes() {
        int emptySize = reversePos - forwardPos + 1;
        if (emptySize == 0) {
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
}
